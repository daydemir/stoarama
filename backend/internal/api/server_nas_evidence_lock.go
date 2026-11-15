package api

// Evidence writers serialize their connection metadata updates. They never
// change a connection key; child-table foreign-key checks may run concurrently.
const nasConnectionEvidenceLock = " FOR NO KEY UPDATE"
