package com.myorg;

import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.services.iam.*;
import software.amazon.awscdk.services.sns.*;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.amazon.awscdk.services.sqs.QueuePolicy;
import software.constructs.Construct;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;

import java.util.List;
import java.util.Map;

public class FifoStack extends Stack {
    public FifoStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public FifoStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        //Notes - publishers to fifo topics set the message group
        // Things to try
        // Can you subscribe a standard queue to a fifo topic? ==> No
        // Can you subscribe a fifo queue to a standard topic? ==> No
        // Can a fifo queue subscribe to multiple fifo topics? ==> yes
        // Message dedup ==> content-based dedup
        // Message dedup vs content-based - AWS recommendations?

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

        Topic topic3 = Topic.Builder.create(this, "topic3")
                .displayName("topic3")
                .topicName("topic3")
                .build();

        Queue sub1dlq = Queue.Builder.create(this, "sub1dlq")
                .fifo(true)
                .queueName("sub1dlq.fifo")
                .visibilityTimeout(Duration.millis(30000))
                .build();


        Queue sub1queue = Queue.Builder.create(this, "sub1Queue")
                .fifo(true)
                .queueName("sub1.fifo")
                .visibilityTimeout(Duration.millis(30000))
                .deadLetterQueue(DeadLetterQueue.builder()
                        .queue(sub1dlq)
                        .maxReceiveCount(3)
                        .build())
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
                .filterPolicy(Map.of(
                        "color", SubscriptionFilter.stringFilter(
                                StringConditions.builder()
                                        .allowlist(List.of("red","blue","green"))
                                        .build()
                        )
                ))
                .build();


        //Fifo queue subscribed to standard topic? ==> No
        //Invalid parameter: Invalid parameter: Endpoint Reason: FIFO SQS Queues can not be subscribed to standard SNS topics (Service: AmazonSNS; Status Code: 400; Error Code:
        //InvalidParameter; Request ID: 6cc2aee4-24c1-5a3e-8644-f4b576516a16; Proxy: null)


        /*Subscription sub3 = Subscription.Builder.create(this, "sub3")
                .protocol(SubscriptionProtocol.SQS)
                .endpoint(sub1queue.getQueueArn())
                .topic(topic3)
                .build();*/


        //Standard queue - sub to fifo?
        //No -11:47:06 AM | CREATE_FAILED        | AWS::SNS::Subscription | q2subC6148ECF
        //Invalid parameter: Invalid parameter: Endpoint Reason: Please use FIFO SQS queue (Service: AmazonSNS; Status Code: 400; Error Code: InvalidParameter; Request ID: 81de
        //fdeb-b62f-5904-8e7f-a7baff55194c; Proxy: null)
        /*Queue sub2queue = Queue.Builder.create(this, "sub2Queue")
                .queueName("sub2q")
                .visibilityTimeout(Duration.millis(30000))
                .build();

        QueuePolicy sub2QueuePolicy = QueuePolicy.Builder.create(this, "sub2QueuePolicy")
                .queues(List.of(sub2queue))
                .build();

        sub2QueuePolicy.getDocument().addStatements(
                PolicyStatement.Builder.create()
                        .actions(List.of("sqs:SendMessage"))
                        .principals(List.of(new ServicePrincipal("sns.amazonaws.com")))
                        .resources(List.of(sub2queue.getQueueArn()))
                        .build()
        );

        Subscription q2sub = Subscription.Builder.create(this, "q2sub")
                .protocol(SubscriptionProtocol.SQS)
                .endpoint(sub2queue.getQueueArn())
                .topic(fifoTopic1)
                .build();

        CfnOutput.Builder.create(this, "s2QueueUrl")
                .value(sub2queue.getQueueUrl())
                .description("subscription 2 queue url")
                .build();*/

        //Standard queue subscribed to standard topic? Of course!
        Queue sub2queue = Queue.Builder.create(this, "sub2Queue")
                .queueName("sub2q")
                .visibilityTimeout(Duration.millis(30000))
                .build();

        QueuePolicy sub2QueuePolicy = QueuePolicy.Builder.create(this, "sub2QueuePolicy")
                .queues(List.of(sub2queue))
                .build();

        sub2QueuePolicy.getDocument().addStatements(
                PolicyStatement.Builder.create()
                        .actions(List.of("sqs:SendMessage"))
                        .principals(List.of(new ServicePrincipal("sns.amazonaws.com")))
                        .resources(List.of(sub2queue.getQueueArn()))
                        .build()
        );

        Subscription q2sub = Subscription.Builder.create(this, "q2sub")
                .protocol(SubscriptionProtocol.SQS)
                .endpoint(sub2queue.getQueueArn())
                .topic(topic3)
                .build();

        CfnOutput.Builder.create(this, "s2QueueUrl")
                .value(sub2queue.getQueueUrl())
                .description("subscription 2 queue url")
                .build();


        //Stack outputs
        CfnOutput.Builder.create(this, "fifoTopic1Arn")
                .value(fifoTopic1.getTopicArn())
                .description("fifoTopic1 arn")
                .build();

        CfnOutput.Builder.create(this, "fifoTopic2Arn")
                .value(fifoTopic2.getTopicArn())
                .description("fifoTopic2 arn")
                .build();

        CfnOutput.Builder.create(this, "topic3Arn")
                .value(topic3.getTopicArn())
                .description("topic3 arn")
                .build();

        CfnOutput.Builder.create(this, "s1QueueUrl")
                .value(sub1queue.getQueueUrl())
                .description("subscription 1 queue url")
                .build();


    }
}
