package client_example

import "time"

const PublisherCount = 1
const PublishCount = 100

var TotalPublishCount = uint64(PublisherCount * PublishCount)

const SubscriberCount = 1

var TotalReceiveCount = TotalPublishCount * SubscriberCount
var StartTime time.Time

const StreamPerSubscriber int = 5
