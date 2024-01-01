package client_example

import "time"

const PublisherCount = 1
const PublishCount = 500

var TotalPublishCount = uint64(PublisherCount * PublishCount)

const SubscriberCount = 500

var TotalReceiveCount = TotalPublishCount * SubscriberCount
var StartTime time.Time

const StreamPerSubscriber int = 1
