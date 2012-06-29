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
using System.Linq;
using System.Reflection;
using System.ServiceModel;
using System.ServiceModel.Description;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace PushTest.BrokeredComponents
{
	public class BrokeredServiceHost : ServiceHost
    {
        const string DefaultNamespace = "http://tempuri.org/";
        ReceiveMode _receiveMode;
        bool _requiresSession;
	    readonly Dictionary<string, string> _actionMap = new Dictionary<string, string>();

        public BrokeredServiceHost(
            object singletonInstance, 
            Uri address,
            TokenProvider tokenProvider,
            ReceiveMode receiveMode,
            bool requiresSession)
            : base(singletonInstance)
        {
            AddBrokeredServiceBehavior(singletonInstance.GetType(), address, tokenProvider, receiveMode, requiresSession);
        }

        public BrokeredServiceHost(
            Type serviceType, 
            Uri address,
            TokenProvider tokenProvider,
            ReceiveMode receiveMode,
            bool requiresSession) 
            : base(serviceType)
        {
            AddBrokeredServiceBehavior(serviceType, address, tokenProvider, receiveMode, requiresSession);
        }

        private void AddBrokeredServiceBehavior(Type serviceType, Uri address, TokenProvider tokenProvider, ReceiveMode receiveMode, bool requiresSession)
        {
            _receiveMode = receiveMode;
            _requiresSession = requiresSession;
            var contractDescription = CreateContractDescription(serviceType);

            var endpoint = new ServiceEndpoint(contractDescription, new BrokeredBinding(), address != null ? new EndpointAddress(address) : null);
            Description.Behaviors.Insert(0, new BrokeredServiceBehavior(endpoint, tokenProvider, receiveMode, _actionMap));
        }

        private ContractDescription CreateContractDescription(Type serviceType)
        {
            var result = new ContractDescription(serviceType.Name, DefaultNamespace) {ContractType = serviceType};

            MethodInfo defaultOnReceiveMessageMethod = null;
            var possibleReturnTypes = new HashSet<Type> { typeof(void), typeof(Task) };

            foreach (var method in serviceType.GetMethods(BindingFlags.Instance | BindingFlags.Public))
            {
                var onMessageAttributes = method.GetCustomAttributes(typeof(OnMessageAttribute), false);
                if (onMessageAttributes.Length <= 0) continue;
                var onMessageAttribute = (OnMessageAttribute)onMessageAttributes[0];
                if (!MethodMatches(method, possibleReturnTypes, typeof (BrokeredMessage))) continue;
                if (onMessageAttribute.Action == OnMessageAttribute.WildcardAction)
                {
                    defaultOnReceiveMessageMethod = method;
                }
                _actionMap.Add(onMessageAttribute.Action, method.Name);
                result.Operations.Add(CreateOperationDescription(result, serviceType, method));
            }

            if (defaultOnReceiveMessageMethod == null)
            {
                defaultOnReceiveMessageMethod = FindConventionOrUniqueMatchingMethod(serviceType, "OnReceiveMessage", possibleReturnTypes, typeof(BrokeredMessage));

                if (defaultOnReceiveMessageMethod != null && defaultOnReceiveMessageMethod.GetCustomAttributes(typeof(OnMessageAttribute), false).Length == 0)
                {
                    _actionMap.Add(OnMessageAttribute.WildcardAction, defaultOnReceiveMessageMethod.Name);
                    result.Operations.Add(CreateOperationDescription(result, serviceType, defaultOnReceiveMessageMethod));
                }
            }

            if (_actionMap.Count == 0)
            {
                throw new Exception("No methods found to handle messages.");
            }

            if (_requiresSession)
            {
                result.SessionMode = SessionMode.Required;
            }

            return result;
        }

        internal static MethodInfo FindConventionOrUniqueMatchingMethod(Type type, string conventionMethodName, HashSet<Type> possibleReturnTypes, params Type[] parameters)
        {
            var method = GetMethod(type, conventionMethodName, possibleReturnTypes, parameters);
            return method ?? FindUniqueMatchingMethod(type, possibleReturnTypes, parameters);
        }

	    internal static MethodInfo GetMethod(Type type, string methodName, HashSet<Type> possibleReturnTypes, params Type[] parameters)
        {
            var method = type.GetMethod(methodName, BindingFlags.Instance | BindingFlags.Public, null, CallingConventions.Any, parameters, null);
            return (method != null && possibleReturnTypes.Contains(method.ReturnType)) ? method : null;
        }

        internal static MethodInfo FindUniqueMatchingMethod(Type type, HashSet<Type> possibleReturnTypes, params Type[] parameters)
        {
            var matchFound = false;
            MethodInfo match = null;
            foreach (var method in type.GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly))
            {
                if (!MethodMatches(method, possibleReturnTypes, parameters)) continue;
                if (!matchFound)
                {
                    matchFound = true;
                    match = method;
                }
                else
                {
                    match = null;
                    break;
                }
            }
            return match;
        }

        internal static bool MethodMatches(MethodInfo method, HashSet<Type> possibleReturnTypes, params Type[] parameterTypes)
        {
            if (possibleReturnTypes.Contains(method.ReturnType))
            {
                var parameters = method.GetParameters();
                if (parameters.Length == parameterTypes.Length)
                {
                    return !parameters.Where((t, i) => t.ParameterType != parameterTypes[i]).Any();
                }
            }
            return false;
        }

        private OperationDescription CreateOperationDescription(ContractDescription contract, Type serviceType, MethodInfo method)
        {
            var operationName = method.Name;
            var parameters = method.GetParameters();
            var result = new OperationDescription(operationName, contract) {SyncMethod = method};

            var inputMessage = new MessageDescription(DefaultNamespace + serviceType.Name + "/" + operationName, MessageDirection.Input);
            inputMessage.Body.WrapperNamespace = DefaultNamespace;
            inputMessage.Body.WrapperName = operationName;
            for (var i = 0; i < parameters.Length; i++)
            {
                var parameter = parameters[i];
                var part = new MessagePartDescription(parameter.Name, DefaultNamespace)
                    {Type = parameter.ParameterType, Index = i};
                inputMessage.Body.Parts.Add(part);
            }

            result.Messages.Add(inputMessage);

            AddOperationBehaviors(result, method);
            return result;
        }

        private void AddOperationBehaviors(OperationDescription result, MethodInfo method)
        {
            if (_receiveMode == ReceiveMode.PeekLock)
            {
                result.Behaviors.Add(new ReceiveContextEnabledAttribute { ManualControl = false });
            }

            foreach (var attribute in method.GetCustomAttributes(false).OfType<IOperationBehavior>())
            {
                result.Behaviors.Add(attribute);
            }
        }
    }
}
