import json
import logging
from typing import Dict, Any
from bl_omniverse_plugin import hsml_to_omniverse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BLOmniverseConsumer:
    """
    Business Logic Consumer for Omniverse: receives HSML data, applies Omniverse plugin, outputs Omniverse format.
    """
    def convert_hsml_to_omniverse(self, hsml_data: Dict[str, Any]) -> Dict[str, Any]:
        return hsml_to_omniverse(hsml_data)

    def send_to_omniverse(self, omniverse_data: Dict[str, Any]):
        logger.info(f"Sending to Omniverse: {omniverse_data}")
        # In a real system, this would send data to Omniverse

# Example usage and testing
def test_bl_omniverse_consumer():
    consumer = BLOmniverseConsumer()
    hsml_test_data = {
        "entity_id": "test_omniverse_agent",
        "position": {"x": 10.0, "y": 5.0, "z": 2.0},
        "rotation": {"x": 0, "y": 0, "z": 0, "w": 1},
        "timestamp": "2024-06-01T12:00:00Z"
    }
    omniverse_result = consumer.convert_hsml_to_omniverse(hsml_test_data)
    print("HSML to Omniverse conversion:")
    print(json.dumps(omniverse_result, indent=2))

if __name__ == "__main__":
    test_bl_omniverse_consumer() 