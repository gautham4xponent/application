import datetime
import time
import uuid
import random
import json

event_status = ["SUCCESS", "ERROR", "STANDBY"]
device_id = ['D' + str(_id).rjust(3, '0') for _id in range(1, 6)]
customer_id = ["CI" + str(_id).rjust(5, '0') for _id in range(100, 121)]


# Generate event data from devices
def generate_events(offset, stream_start_time):

    _event = {
        "eventId": str(uuid.uuid4()),
        "eventOffset": offset,
        "eventPublisher": "device",
        "customerId": random.choice(customer_id),

        # Stream level metadata
        "streamStartTime": stream_start_time,

        "data": {
            "devices": [
                {
                    "deviceId": random.choice(device_id),
                    "temperature": random.randint(0, 30),
                    "measure": "C",
                    "status": random.choice(event_status)
                }
                for i in range(random.randint(1, 3))
            ]
        },

        "eventTime": datetime.datetime.now(
            datetime.timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
    }

    return json.dumps(
    _event,
    default=lambda x: x.isoformat()
)


if __name__ == "__main__":

    _offset = 10000

    test_stream_start_time = datetime.datetime.now(
        datetime.timezone.utc
    ).isoformat(timespec="milliseconds")

    while True:
        print(
            generate_events(
                _offset,
                test_stream_start_time
            )
        )

        time.sleep(random.randint(0, 5))
        _offset += 1