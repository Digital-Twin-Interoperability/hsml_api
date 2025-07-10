import json
from bl_omniverse_plugin import hsml_to_omniverse
from bl_omniverse_producer import BLOmniverseProducer
from bl_omniverse_consumer import BLOmniverseConsumer

def test_omniverse_end_to_end():
    print("\n=== Omniverse BL Integration Test ===")
    # 1. Create Omniverse native data
    omniverse_data = {
        "id": "omniverse_agent_001",
        "transform": {
            "translation": [20.0, 10.0, 5.0],
            "rotation": [0, 0, 0, 1]
        },
        "time": "2024-06-01T12:00:00Z"
    }
    print("\nOmniverse Native Data:")
    print(json.dumps(omniverse_data, indent=2))
    # 2. Producer: Omniverse to HSML
    producer = BLOmniverseProducer()
    hsml = producer.convert_omniverse_to_hsml(omniverse_data)
    print("\nConverted to HSML:")
    print(json.dumps(hsml, indent=2))
    # 3. Plugin: HSML to Omniverse
    omniverse_result = hsml_to_omniverse(hsml)
    print("\nHSML to Omniverse (Plugin):")
    print(json.dumps(omniverse_result, indent=2))
    # 4. Consumer: Simulate receiving HSML and outputting Omniverse
    consumer = BLOmniverseConsumer()
    print("\nConsumer Output:")
    consumer.send_to_omniverse(omniverse_result)
    print("\n=== End of Omniverse BL Integration Test ===\n")

if __name__ == "__main__":
    test_omniverse_end_to_end() 