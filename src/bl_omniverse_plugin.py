import json

def hsml_to_omniverse(hsml_data):
    """
    Convert HSML movement data to Omniverse format.
    :param hsml_data: dict or JSON string
    :return: dict in Omniverse format
    """
    if isinstance(hsml_data, str):
        hsml_data = json.loads(hsml_data)

    omniverse_data = {
        "id": hsml_data["entity_id"],
        "transform": {
            "translation": [
                hsml_data["position"]["x"],
                hsml_data["position"]["y"],
                hsml_data["position"]["z"]
            ],
            "rotation": [
                hsml_data["rotation"]["x"],
                hsml_data["rotation"]["y"],
                hsml_data["rotation"]["z"],
                hsml_data["rotation"].get("w", 1)
            ] if "rotation" in hsml_data else [0, 0, 0, 1]
        },
        "time": hsml_data.get("timestamp")
    }
    return omniverse_data

# Example usage
def main():
    hsml_example = {
        "entity_id": "agent_001",
        "position": {"x": 1.0, "y": 2.0, "z": 3.0},
        "rotation": {"x": 0, "y": 0, "z": 0, "w": 1},
        "timestamp": "2024-06-01T12:00:00Z"
    }
    converted = hsml_to_omniverse(hsml_example)
    print(json.dumps(converted, indent=2))

if __name__ == "__main__":
    main() 