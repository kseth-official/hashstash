package constants

const SynchronousLogger = true

const (
	LogRaftStart = iota
	LogElection
	LogStart
	LogAppendEntries
	LogRejectAppendEntries
	LogMatchPreviousEntryAppendEntries
	LogTruncateLogAppendEntries
	LogUpdateAppendEntries
	LogAppendingAppendEntries
	LogUpdateCommitIndexAppendEntries
	LogCommittingEntriesAppendEntries
	LogHeartbeat
	LogSyncLogEntries

	// From lab4c_ksethapp
	LogTerm
	LogVote
	LogCandidate
	LogTimer
	LogLeader
	LogEntryAdded
	LogEncodingError
	LogDecodingError
	LogPersist
	LogReadPersist
	// TODO: Add more log types here
)

const (
	LogClerk = iota
	LogServer
)

var RaftLoggingMap = map[int]string{
	LogRaftStart:                       "RaftStartEvent",
	LogElection:                        "ElectionEvent",
	LogStart:                           "StartEvent",
	LogAppendEntries:                   "AppendEntriesEvent",
	LogRejectAppendEntries:             "RejectAppendEntriesEvent",
	LogMatchPreviousEntryAppendEntries: "MatchPreviousEntryAppendEntriesEvent",
	LogTruncateLogAppendEntries:        "TruncateLogAppendEntriesEvent",
	LogUpdateAppendEntries:             "UpdateAppendEntriesEvent",
	LogAppendingAppendEntries:          "AppendingAppendEntriesEvent",
	LogUpdateCommitIndexAppendEntries:  "UpdateCommitIndexAppendEntriesEvent",
	LogCommittingEntriesAppendEntries:  "CommittingEntriesAppendEntriesEvent",
	LogHeartbeat:                       "HeartbeatEvent",
	LogSyncLogEntries:                  "SyncLogEntriesEvent",

	// From lab4c_ksethapp
	LogTerm:          "RaftTermEvent",
	LogVote:          "RaftVoteEvent",
	LogCandidate:     "RaftCandidateEvent",
	LogTimer:         "RaftTimerEvent",
	LogLeader:        "RaftLeaderEvent",
	LogEntryAdded:    "LogEntryAddedEvent",
	LogEncodingError: "LogEncodingErrorEvent",
	LogDecodingError: "LogDecodingErrorEvent",
	LogPersist:       "LogPersistEvent",
	LogReadPersist:   "LogReadPersistEvent",
	// TODO: Add more log types here
}

var ClerkLoggingMap = map[int]string{
	LogClerk:  "ClerkEvent",
	LogServer: "ServerEvent",
	// TODO: Add more log types here
}
