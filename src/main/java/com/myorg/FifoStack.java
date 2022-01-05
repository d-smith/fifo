package com.myorg;

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.services.iam.AccountPrincipal;
import software.amazon.awscdk.services.iam.AccountRootPrincipal;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.sns.Subscription;
import software.amazon.awscdk.services.sns.SubscriptionProtocol;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sns.TopicPolicy;
import software.amazon.awscdk.services.sqs.Queue;
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

        Topic userRegistration = Topic.Builder.create(this, "userregTopic")
                .fifo(true)
                .displayName("userreg")
                .topicName("userreg")
                .build();

        TopicPolicy topicPolicy = TopicPolicy.Builder.create(this, "topicPolicy")
                .topics(List.of(userRegistration))
                .build();

        topicPolicy.getDocument().addStatements(PolicyStatement.Builder.create()
            .actions(List.of("sns:Subscribe"))
                .principals(List.of(new AccountPrincipal(this.getAccount())))
                .resources(List.of(userRegistration.getTopicArn()))
                .build()
        );

        Queue sub1queue = Queue.Builder.create(this, "sub1Queue")
                .fifo(true)
                .queueName("sub1.fifo")
                .visibilityTimeout(Duration.millis(30000))
                .build();

        Subscription sub1 = Subscription.Builder.create(this, "sub1")
                .protocol(SubscriptionProtocol.SQS)
                .endpoint(sub1queue.getQueueArn())
                .topic(userRegistration)
                .build();



    }
}
