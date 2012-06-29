//---------------------------------------------------------------------------------
// Microsoft (R)  Windows Azure ServiceBus Messaging sample
// 
// Copyright (c) Microsoft Corporation. All rights reserved.  
//
// THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, 
// EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES 
// OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE. 
//---------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.ServiceModel;
using Microsoft.ServiceBus.Messaging;

namespace PushTest.BrokeredComponents
{
	public delegate void OnReceiveMessageCallback(BrokeredMessage message);

    static class ServiceBusExtensions
    {
        static readonly Dictionary<QueueClient, BrokeredServiceHost> RegisteredQueueHosts = new Dictionary<QueueClient, BrokeredServiceHost>();
        static readonly Dictionary<SubscriptionClient, BrokeredServiceHost> RegisteredSubscriptionHosts = new Dictionary<SubscriptionClient, BrokeredServiceHost>();

        public static void SetAction(this BrokeredMessage message, string action)
        {
            message.Properties.Add(BrokeredOperationSelector.Action, action);
        }

        public static void StartListener(this QueueClient queueClient, Type serviceType, ReceiveMode receiveMode = ReceiveMode.PeekLock, bool requiresSession = false)
        {
            var address = new Uri(queueClient.MessagingFactory.Address, queueClient.Path);
            var host = new BrokeredServiceHost(serviceType, address, queueClient.MessagingFactory.GetSettings().TokenProvider, receiveMode, requiresSession);

            RegisteredQueueHosts.Add(queueClient, host);
            host.Open();
        }

        public static void StartListener(this QueueClient queueClient, object singletonInstance, ReceiveMode receiveMode = ReceiveMode.PeekLock, bool requiresSession = false)
        {
            var address = new Uri(queueClient.MessagingFactory.Address, queueClient.Path);
            var host = new BrokeredServiceHost(singletonInstance, address, queueClient.MessagingFactory.GetSettings().TokenProvider, receiveMode, requiresSession);

            RegisteredQueueHosts.Add(queueClient, host);
            host.Open();
        }

        public static void StartListener(this QueueClient queueClient, OnReceiveMessageCallback onReceiveMessageCallback, ReceiveMode receiveMode = ReceiveMode.PeekLock, bool requiresSession = false)
        {
            queueClient.StartListener(new OnReceiveMessageCallbackDelegator(onReceiveMessageCallback), receiveMode, requiresSession);
        }

        public static void StartListener(this SubscriptionClient subscriptionClient, Type serviceType, ReceiveMode receiveMode = ReceiveMode.PeekLock, bool requiresSession = false)
        {
            var address = new Uri(subscriptionClient.MessagingFactory.Address, subscriptionClient.TopicPath + "/Subscriptions/" + subscriptionClient.Name);
            var host = new BrokeredServiceHost(serviceType, address, subscriptionClient.MessagingFactory.GetSettings().TokenProvider, receiveMode, requiresSession);

            RegisteredSubscriptionHosts.Add(subscriptionClient, host);
            host.Open();
        }

        public static void StartListener(this SubscriptionClient subscriptionClient, object singletonInstance, ReceiveMode receiveMode = ReceiveMode.PeekLock, bool requiresSession = false)
        {
            var address = new Uri(subscriptionClient.MessagingFactory.Address, subscriptionClient.TopicPath + "/Subscriptions/" + subscriptionClient.Name);
            var host = new BrokeredServiceHost(singletonInstance, address, subscriptionClient.MessagingFactory.GetSettings().TokenProvider, receiveMode, requiresSession);

            RegisteredSubscriptionHosts.Add(subscriptionClient, host);
            host.Open();
        }

        public static void StartListener(this SubscriptionClient subscriptionClient, OnReceiveMessageCallback onReceiveMessageCallback, ReceiveMode receiveMode = ReceiveMode.PeekLock, bool requiresSession = false)
        {
            subscriptionClient.StartListener(new OnReceiveMessageCallbackDelegator(onReceiveMessageCallback), receiveMode, requiresSession);
        }

        public static void StopListener(this QueueClient queueClient)
        {
            BrokeredServiceHost host;
            bool found = RegisteredQueueHosts.TryGetValue(queueClient, out host);
            if (found)
            {
                host.Close();
            }
            else
            {
                throw new Exception("No handler registered");
            }
        }

        public static void StopListener(this SubscriptionClient subscriptionClient)
        {
            BrokeredServiceHost host;
            bool found = RegisteredSubscriptionHosts.TryGetValue(subscriptionClient, out host);
            if (found)
            {
                host.Close();
            }
            else
            {
                throw new Exception("No handler registered");
            }
        }

        [ServiceBehavior(InstanceContextMode=InstanceContextMode.Single)]
        class OnReceiveMessageCallbackDelegator
        {
        	readonly OnReceiveMessageCallback _callback;
            
            public OnReceiveMessageCallbackDelegator(OnReceiveMessageCallback callback)
            {
                _callback = callback;
            }

            public void OnReceiveMessage(BrokeredMessage message)
            {
                _callback(message);
            }
        }
    }
}
