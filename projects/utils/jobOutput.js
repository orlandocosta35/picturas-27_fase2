const { createHash } = require('node:crypto');

// The unique MongoDB _id makes a confirmed reply safe to deliver again after a
// disconnect, including a disconnect between saving a Result and deleting Process.
function jobOutputId(messageId, fileName) {
  return createHash('sha256').update(JSON.stringify([messageId, fileName])).digest('hex').slice(0, 24);
}

async function saveJobOutput(Model, output, messageId) {
  return Model.findOneAndUpdate(
    { _id: jobOutputId(messageId, output.file_name) },
    { $setOnInsert: output },
    { upsert: true, new: true, setDefaultsOnInsert: true },
  ).exec();
}

module.exports = { jobOutputId, saveJobOutput };
