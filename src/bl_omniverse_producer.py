import json
import logging
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BLOmniverseProducer:
    """
    Business Logic Producer for Omniverse: converts Omniverse data to HSML.
    """
    def convert_omniverse_to_hsml(self, omniverse_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert Omniverse format to HSML format.
        """
        hsml_data = {
            "entity_id": omniverse_data.get("id", omniverse_data.get("entity_id")),
            "position": {
                "x": omniverse_data["transform"]["translation"][0],
                "y": omniverse_data["transform"]["translation"][1],
                "z": omniverse_data["transform"]["translation"][2]
            },
            "rotation": {
                "x": omniverse_data["transform"]["rotation"][0],
                "y": omniverse_data["transform"]["rotation"][1],
                "z": omniverse_data["transform"]["rotation"][2],
                "w": omniverse_data["transform"]["rotation"][3]
            },
            "timestamp": omniverse_data.get("time", omniverse_data.get("timestamp"))
        }
        return hsml_data

# Example usage and testing
def test_bl_omniverse_producer():
    producer = BLOmniverseProducer()
    omniverse_test_data = {
        "id": "omniverse_agent_001",
        "transform": {
            "translation": [20.0, 10.0, 5.0],
            "rotation": [0, 0, 0, 1]
        },
        "time": "2024-06-01T12:00:00Z"
    }
    hsml = producer.convert_omniverse_to_hsml(omniverse_test_data)
    print("Omniverse to HSML conversion:")
    print(json.dumps(hsml, indent=2))

if __name__ == "__main__":
    test_bl_omniverse_producer() 