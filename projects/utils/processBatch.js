// Publish only after every source has a persisted Process record. A fast worker
// must never see an incomplete batch when deciding whether the run is finished.
async function dispatchProcessBatch(jobs, { create, remove, send }) {
  const created = [];
  try {
    for (const job of jobs) created.push(await create(job.process));
  } catch (error) {
    await Promise.allSettled(created.map(remove));
    throw error;
  }
  for (const job of jobs) await send(job);
}

module.exports = { dispatchProcessBatch };

// Keep the last source as a durable completion marker until both the commit and
// notification are confirmed. A redelivery can safely replay either operation.
async function completeProcessSource({ retained, countPending, commit, notify, remove }) {
  if (await countPending() === (retained ? 1 : 0)) {
    if (!await commit()) {
      if (retained) await remove();
      return false;
    }
  }
  if (await notify() === false) {
    if (retained) await remove();
    return false;
  }
  if (retained) await remove();
  return true;
}

async function failProcessSource({ preview, token, invalidate, getProject, notify, remove, removeRun }) {
  if (!preview) {
    const invalidated = await invalidate();
    if (!invalidated) {
      const project = await getProject();
      // Resume an interrupted error notification, but never cancel a newer run
      // or invalidate results that this run has already committed.
      if (!project || project.failedToken !== token || project.activeToken !== token + 1) {
        await remove();
        return false;
      }
    }
  }
  await notify();
  if (preview) await remove();
  else await removeRun();
  return true;
}

module.exports.completeProcessSource = completeProcessSource;
module.exports.failProcessSource = failProcessSource;

// Some confirmed jobs may already be running when a later publication fails.
// Invalidate the run before removing barriers, so late replies cannot commit it.
async function abortDispatchedRun({ token, invalidate, getProject, removeRun }) {
  const project = await invalidate() || await getProject();
  // An ambiguous publish confirmation may arrive after the entire run has
  // actually committed. Keep its completion marker for notification replay.
  if (project && project.committedToken !== token) await removeRun();
}

module.exports.abortDispatchedRun = abortDispatchedRun;

// A replay after commit must never touch output files again: a failed upload or
// rollback could otherwise remove results that are already visible to the user.
async function resumeCommittedSource({ committed, notify, remove }) {
  if (!committed) return false;
  await notify();
  await remove();
  return true;
}

module.exports.resumeCommittedSource = resumeCommittedSource;
