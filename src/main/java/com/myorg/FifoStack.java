package com.myorg;

import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.services.iam.*;
import software.amazon.awscdk.services.sns.Subscription;
import software.amazon.awscdk.services.sns.SubscriptionProtocol;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.Queue;
import software.amazon.awscdk.services.sqs.QueuePolicy;
import software.constructs.Construct;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;

import java.util.List;

public class FifoStack extends Stack {
    public FifoStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public FifoStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        //Notes - publishers to fifo topics set the message group
        // Things to try
        // Can you subscribe a standard queue to a fifo topic?
        // Can a fifo queue subscribe to multiple fifo topics?
        // Message dedup

        Topic fifoTopic1 = Topic.Builder.create(this, "fifoTopic1")
                .fifo(true)
                .displayName("fifoTopic1")
                .topicName("fifoTopic1")
                .contentBasedDeduplication(true)
                .build();

        Topic fifoTopic2 = Topic.Builder.create(this, "fifoTopic2")
                .fifo(true)
                .displayName("fifoTopic2")
                .topicName("fifoTopic2")
                .contentBasedDeduplication(true)
                .build();

        Queue sub1queue = Queue.Builder.create(this, "sub1Queue")
                .fifo(true)
                .queueName("sub1.fifo")
                .visibilityTimeout(Duration.millis(30000))
                .build();

        QueuePolicy sub1QueuePolicy = QueuePolicy.Builder.create(this, "sub1QueuePolicy")
                .queues(List.of(sub1queue))
                .build();

        sub1QueuePolicy.getDocument().addStatements(
                PolicyStatement.Builder.create()
                        .actions(List.of("sqs:SendMessage"))
                        .principals(List.of(new ServicePrincipal("sns.amazonaws.com")))
                        .resources(List.of(sub1queue.getQueueArn()))
                        .build()
        );

        Subscription sub1 = Subscription.Builder.create(this, "sub1")
                .protocol(SubscriptionProtocol.SQS)
                .endpoint(sub1queue.getQueueArn())
                .topic(fifoTopic1)
                .build();

        Subscription sub2 = Subscription.Builder.create(this, "sub2")
                .protocol(SubscriptionProtocol.SQS)
                .endpoint(sub1queue.getQueueArn())
                .topic(fifoTopic2)
                .build();

        CfnOutput.Builder.create(this, "fifoTopic1Arn")
                .value(fifoTopic1.getTopicArn())
                .description("fifoTopic1 arn")
                .build();

        CfnOutput.Builder.create(this, "fifoTopic2Arn")
                .value(fifoTopic2.getTopicArn())
                .description("fifoTopic2 arn")
                .build();

        CfnOutput.Builder.create(this, "s1QueueUrl")
                .value(sub1queue.getQueueUrl())
                .description("subscription 1 queue url")
                .build();
    }
}
