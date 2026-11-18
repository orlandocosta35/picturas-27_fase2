const test = require('node:test');
const assert = require('node:assert/strict');
const { dispatchProcessBatch } = require('../utils/processBatch');

// Simulate workers completing immediately when published, which used to permit
// the first source to commit before the second source had even been registered.
test('a fast first worker cannot commit while later sources are still being scheduled', async () => {
  const pending = new Set();
  const completionCounts = [];
  const jobs = ['video-a', 'video-b', 'image-c'].map(id => ({ process: { _id: id } }));
  await dispatchProcessBatch(jobs, {
    create: async process => { pending.add(process._id); return process; },
    remove: async process => pending.delete(process._id),
    send: async job => {
      pending.delete(job.process._id);
      completionCounts.push(pending.size);
    },
  });
  assert.deepEqual(completionCounts, [2, 1, 0]);
});

test('registration failure starts no workers and rolls back only newly registered jobs', async () => {
  const pending = new Set(['previous-run']);
  const published = [];
  const jobs = ['first', 'failing', 'last'].map(id => ({ process: { _id: id } }));
  await assert.rejects(dispatchProcessBatch(jobs, {
    create: async process => {
      if (process._id === 'failing') throw new Error('database unavailable');
      pending.add(process._id);
      return process;
    },
    remove: async process => pending.delete(process._id),
    send: async job => published.push(job),
  }), /database unavailable/);
  assert.deepEqual(published, []);
  assert.deepEqual([...pending], ['previous-run']);
});

test('a cancelled run cannot overwrite the committed results with a delayed commit', async () => {
  const modelPath = require.resolve('../models/project');
  const controllerPath = require.resolve('../controllers/project');
  const originalModel = require.cache[modelPath];
  const originalController = require.cache[controllerPath];
  const state = { user_id: 'owner', _id: 'project', activeToken: 8, committedToken: 6 };
  require.cache[modelPath] = { id: modelPath, filename: modelPath, loaded: true, exports: {
    findOneAndUpdate(filter, update) {
      return { exec: async () => {
        if (Object.entries(filter).some(([key, value]) => state[key] !== value)) return null;
        Object.assign(state, update.$set);
        return { ...state };
      }};
    },
  }};
  delete require.cache[controllerPath];
  try {
    const Project = require('../controllers/project');
    assert.equal(await Project.commitRunIfActive('owner', 'project', 7, []), null);
    assert.equal(state.committedToken, 6);
    const result = await Project.commitRunIfActive('owner', 'project', 8, [{ procedure: 'extract_frames' }]);
    assert.equal(result.committedToken, 8);
    assert.equal(result.resultsToken, 8);
  } finally {
    if (originalModel) require.cache[modelPath] = originalModel; else delete require.cache[modelPath];
    if (originalController) require.cache[controllerPath] = originalController; else delete require.cache[controllerPath];
  }
});

const { completeProcessSource, failProcessSource } = require('../utils/processBatch');

test('the last source retains its barrier until commit and confirmed notification, including replay', async () => {
  const events = [];
  let pending = 1;
  let notificationFails = true;
  const steps = {
    retained: true,
    countPending: async () => pending,
    commit: async () => { events.push('commit'); return {}; },
    notify: async () => {
      events.push('notify');
      if (notificationFails) throw new Error('broker disconnected');
    },
    remove: async () => { events.push('remove'); pending--; },
  };
  await assert.rejects(completeProcessSource(steps), /broker disconnected/);
  assert.equal(pending, 1);
  assert.deepEqual(events, ['commit', 'notify']);
  notificationFails = false;
  await completeProcessSource(steps);
  assert.deepEqual(events, ['commit', 'notify', 'commit', 'notify', 'remove']);
  assert.equal(pending, 0);
});

test('an early source cannot commit before the other pending source finishes', async () => {
  const events = [];
  await completeProcessSource({
    retained: true,
    countPending: async () => 2,
    commit: async () => { events.push('commit'); },
    notify: async () => { events.push('notify'); },
    remove: async () => { events.push('remove'); },
  });
  assert.deepEqual(events, ['notify', 'remove']);
});

test('terminal failure invalidates a run before clearing barriers and preserves its previous commit', async () => {
  const state = { activeToken: 8, committedToken: 6, resultsToken: 6, failedToken: -1 };
  const pending = new Set(['video-a', 'video-b']);
  const events = [];
  let failNotification = true;
  const steps = {
    preview: false,
    token: 8,
    invalidate: async () => {
      if (state.activeToken !== 8) return null;
      events.push('invalidate');
      state.activeToken++;
      state.failedToken = 8;
      return state;
    },
    getProject: async () => state,
    notify: async () => {
      events.push('notify');
      if (failNotification) throw new Error('broker disconnected');
    },
    remove: async () => pending.delete('video-a'),
    removeRun: async () => { events.push('remove-run'); pending.clear(); },
  };
  await assert.rejects(failProcessSource(steps), /broker disconnected/);
  assert.equal(state.activeToken, 9);
  assert.equal(pending.size, 2);
  failNotification = false;
  await failProcessSource(steps);
  assert.deepEqual(events, ['invalidate', 'notify', 'notify', 'remove-run']);
  assert.equal(state.committedToken, 6);
  assert.equal(state.resultsToken, 6);
  assert.equal(pending.size, 0);
});

test('a delayed terminal error cannot invalidate or notify a newer run', async () => {
  const events = [];
  await failProcessSource({
    preview: false,
    token: 8,
    invalidate: async () => null,
    getProject: async () => ({ activeToken: 10, failedToken: 8, committedToken: 6 }),
    notify: async () => events.push('notify'),
    remove: async () => events.push('remove-old'),
    removeRun: async () => events.push('remove-run'),
  });
  assert.deepEqual(events, ['remove-old']);
});

test('a failed preview removes only its own job after notification', async () => {
  const events = [];
  await failProcessSource({
    preview: true,
    token: 8,
    invalidate: async () => events.push('invalidate'),
    getProject: async () => { throw new Error('must not query a failed run'); },
    notify: async () => events.push('notify'),
    remove: async () => events.push('remove-preview'),
    removeRun: async () => events.push('remove-run'),
  });
  assert.deepEqual(events, ['notify', 'remove-preview']);
});

test('atomic failure transition rejects stale errors and never changes committed results', async () => {
  const modelPath = require.resolve('../models/project');
  const controllerPath = require.resolve('../controllers/project');
  const originalModel = require.cache[modelPath];
  const originalController = require.cache[controllerPath];
  const state = { user_id: 'owner', _id: 'project', activeToken: 8, committedToken: 6, resultsToken: 6, version: 3 };
  require.cache[modelPath] = { id: modelPath, filename: modelPath, loaded: true, exports: {
    findOneAndUpdate(filter, update) {
      return { exec: async () => {
        if (Object.entries(filter).some(([key, value]) =>
          value && typeof value === 'object' && '$ne' in value ? state[key] === value.$ne : state[key] !== value)) return null;
        Object.assign(state, update.$set);
        for (const [key, value] of Object.entries(update.$inc || {})) state[key] = (state[key] || 0) + value;
        return { ...state };
      } };
    },
  } };
  delete require.cache[controllerPath];
  try {
    const Project = require('../controllers/project');
    assert.equal(await Project.failRunIfActive('owner', 'project', 7), null);
    const failed = await Project.failRunIfActive('owner', 'project', 8);
    assert.equal(failed.activeToken, 9);
    assert.equal(failed.failedToken, 8);
    assert.equal(failed.version, 4);
    assert.equal(failed.committedToken, 6);
    assert.equal(failed.resultsToken, 6);
    assert.equal(await Project.failRunIfActive('owner', 'project', 8), null);
    state.activeToken = state.committedToken = 10;
    assert.equal(await Project.failRunIfActive('owner', 'project', 10), null);
  } finally {
    if (originalModel) require.cache[modelPath] = originalModel; else delete require.cache[modelPath];
    if (originalController) require.cache[controllerPath] = originalController; else delete require.cache[controllerPath];
  }
});

const { abortDispatchedRun } = require('../utils/processBatch');

test('partial publication failure invalidates the run before clearing its barriers', async () => {
  const state = { activeToken: 8, committedToken: 6, resultsToken: 6 };
  const pending = new Set(['previous-run']);
  const sent = [];
  const order = [];
  const jobs = ['first', 'failing', 'last'].map(id => ({ process: { _id: id } }));
  try {
    await dispatchProcessBatch(jobs, {
      create: async job => { pending.add(job._id); return job; },
      remove: async job => pending.delete(job._id),
      send: async job => {
        if (job.process._id === 'failing') throw new Error('publication not confirmed');
        sent.push(job.process._id);
      },
    });
    assert.fail('expected partial dispatch failure');
  } catch (error) {
    assert.match(error.message, /publication not confirmed/);
    await abortDispatchedRun({
      token: 8,
      invalidate: async () => { order.push('invalidate'); state.activeToken++; return state; },
      getProject: async () => state,
      removeRun: async () => {
        assert.equal(state.activeToken, 9);
        order.push('remove');
        for (const job of jobs) pending.delete(job.process._id);
      },
    });
  }
  assert.deepEqual(sent, ['first']);
  assert.deepEqual(order, ['invalidate', 'remove']);
  assert.deepEqual([...pending], ['previous-run']);
  assert.equal(state.committedToken, 6);
  assert.equal(state.resultsToken, 6);
  assert.notEqual(state.activeToken, 8); // late workers cannot commit token 8
});

test('ambiguous publication failure does not remove the marker of an already committed run', async () => {
  const state = { activeToken: 8, committedToken: 8 };
  let removed = false;
  await abortDispatchedRun({
    token: 8,
    invalidate: async () => null,
    getProject: async () => state,
    removeRun: async () => { removed = true; },
  });
  assert.equal(removed, false);
});

test('image completion without a retained row still waits for all other media', async () => {
  const events = [];
  const steps = {
    retained: false,
    countPending: async () => 1,
    commit: async () => { events.push('commit'); return {}; },
    notify: async () => { events.push('notify'); },
    remove: async () => { throw new Error('image row was already removed'); },
  };
  await completeProcessSource(steps);
  assert.deepEqual(events, ['notify']);
  steps.countPending = async () => 0;
  await completeProcessSource(steps);
  assert.deepEqual(events, ['notify', 'commit', 'notify']);
});

const { resumeCommittedSource } = require('../utils/processBatch');

test('a committed video replay never persists or rolls back visible frame outputs', async () => {
  const state = { committed: true, row: true };
  const events = [];
  let notificationFails = true;
  async function handleReply() {
    if (await resumeCommittedSource({
      committed: state.committed,
      notify: async () => {
        events.push('notify');
        if (notificationFails) throw new Error('broker disconnected');
      },
      remove: async () => { events.push('remove'); state.row = false; },
    })) return;
    events.push('persist');
    throw new Error('output files must not be opened after commit');
  }
  await assert.rejects(handleReply(), /broker disconnected/);
  assert.equal(state.row, true);
  notificationFails = false;
  await handleReply();
  assert.equal(state.row, false);
  assert.deepEqual(events, ['notify', 'notify', 'remove']);
});
