from sklearn.linear_model import SGDClassifier
from sklearn.preprocessing import MinMaxScaler
import pandas as pd
import numpy as np
from ast import literal_eval


from pubnub.callbacks import SubscribeCallback
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub
from pubnub.enums import PNOperationType, PNStatusCategory

pnconfig = PNConfiguration()
pnconfig.subscribe_key = "sub-c-8a8e493c-f876-11e6-80ea-0619f8945a4f"
pnconfig.publish_key = "pub-c-a64b528c-0749-416f-bf75-50abbfa905f9"
pnconfig.ssl = False
pubnub = PubNub(pnconfig)


learner = SGDClassifier(loss='log')
minmax_scaler = MinMaxScaler(feature_range=(0, 1))
cumulative_accuracy = list()

def publish_callback(result, status):
    pass
    # Handle PNPublishResult and PNStatus

class MySubscribeCallback(SubscribeCallback):
    m = -1
    learner = SGDClassifier(loss='log', shuffle = True, learning_rate = 'optimal')
    minmax_scaler = MinMaxScaler(feature_range=(0, 1))
    cumulative_accuracy = list()

    def __init__(self):
            m = -1
            learner = SGDClassifier(loss='log')
            minmax_scaler = MinMaxScaler(feature_range=(0, 1))
            cumulative_accuracy = list()
    def status(self, pubnub, status):
        pass
        # The status object returned is always related to subscribe but could contain
        # information about subscribe, heartbeat, or errors
        # use the operationType to switch on different options
        if status.operation == PNOperationType.PNSubscribeOperation \
                or status.operation == PNOperationType.PNUnsubscribeOperation:
            if status.category == PNStatusCategory.PNConnectedCategory:
                pass
                # This is expected for a subscribe, this means there is no error or issue whatsoever
            elif status.category == PNStatusCategory.PNReconnectedCategory:
                pass
                # This usually occurs if subscribe temporarily fails but reconnects. This means
                # there was an error but there is no longer any issue
            elif status.category == PNStatusCategory.PNDisconnectedCategory:
                pass
                # This is the expected category for an unsubscribe. This means here
                # was no error in unsubscribing from everything
            elif status.category == PNStatusCategory.PNUnexpectedDisconnectCategory:
                pass
                # This is usually an issue with the internet connection, this is an error, handle
                # appropriately retry will be called automatically
            elif status.category == PNStatusCategory.PNAccessDeniedCategory:
                pass
                # This means that PAM does not allow this client to subscribe to this
                # channel and channel group configuration. This is another explicit error
            else:
                pass
                # This is usually an issue with the internet connection, this is an error, handle appropriately
                # retry will be called automatically
        elif status.operation == PNOperationType.PNSubscribeOperation:
            # Heartbeat operations can in fact have errors, so it is important to check first for an error.
            # For more information on how to configure heartbeat notifications through the status
            # PNObjectEventListener callback, consult <link to the PNCONFIGURATION heartbeart config>
            if status.is_error():
                pass
                # There was an error with the heartbeat operation, handle here
            else:
                pass
                # Heartbeat operation was successful
        else:
            pass
            # Encountered unknown status type
 
    def presence(self, pubnub, presence):
        pass  # handle incoming presence data
    def message(self, pubnub, message):
        self.m += 1
        chunk = literal_eval(message.message['chunk'])
        chunk = np.array(chunk)
        print('learning epoch #', self.m)

        if self.m == 0:
            minmax_scaler.fit(chunk[:,1:])
        X = minmax_scaler.transform(chunk[:,1:])
        X[X>1] = 1
        X[X<0] = 0
        y = chunk[:,0]   
        
        if self.m > 8 :
            cumulative_accuracy.append(learner.score(X,y))

        learner.partial_fit(X,y,classes=[0,1])
        print('Progressive validation mean accuracy %0.3f' % np.mean(cumulative_accuracy))
 
 
pubnub.add_listener(MySubscribeCallback())
pubnub.subscribe().channels('ch1').execute()


streaming = pd.read_csv('huge_dataset_10__7.csv', header=None, chunksize=200)

for n,chunk in enumerate(streaming):

    chunk = np.array(chunk).tolist()
    dictionary = {'chunk': str(chunk), 'n': n} 
    pubnub.publish().channel('ch1').message(dictionary).pn_async(publish_callback)
        

   
print('Progressive validation mean accuracy %0.3f' % np.mean(cumulative_accuracy))
print('DONE')