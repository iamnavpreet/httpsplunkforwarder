import json
import requests


class SplunkForwarder:
    def __init__(self, authorization_token, splunk_ingester_domain, connection_port='443'):
        assert 'http' in splunk_ingester_domain
        assert authorization_token
        self.token = authorization_token
        self.ingester_url = "{}:{}{}".format(splunk_ingester_domain, connection_port, "/services/collector/event")
        self.port = connection_port

    def build_metadata(self, index_name=None, **other_metadata):
        metadata = dict()
        if index_name:
            metadata["index"] = index_name

        if other_metadata:
            metadata.update(other_metadata)

    def send(self):
        headers = dict()
        headers['Authorization'] = 'Splunk {}'.format(self.token)

        if hasattr(self, "payload") and self.payload:
            response = requests.post(self.ingester_url, data=self.payload, headers=headers)
            if response.status_code != 200:
                print("Issues in sending to splunk - URL -> {}".format(self.ingester_url))

    def build_payload(self, events, metadata):

        if isinstance(events, list):
            concatenated_payload = ""
            for event in events:
                payload = dict()
                payload["host"] = self.ingester_url
                payload["event"] = event

                if metadata:
                    payload.update(metadata)
                concatenated_payload += json.dumps(payload)

            if concatenated_payload:
                setattr(self, "payload", concatenated_payload)
                # r = requests.post(self.ingester_url?, data=concatenated_payload, headers=headers)
        else:

            payload = dict()
            payload["host"] = self.ingester_url
            payload["event"] = events

            if metadata:
                payload.update(metadata)

            setattr(self, "payload", json.dumps(payload))
