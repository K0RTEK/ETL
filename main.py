import json
from pprint import pprint
from xml.etree import ElementTree as ET

def flatten_nutrition_data(nutrition_data_path: str):
    tree = ET.parse(nutrition_data_path)
    root = tree.getroot()

    food_tags = [child.tag for child in root.find("food")]
    daily_tags = [child.tag for child in root.find("daily-values")]
    result = []

    for daily in root.findall("daily-values"):
        flatten_daily_data = {}
        for tag in daily_tags:
            flatten_daily_data[tag + f".{daily.find(tag).attrib['units']}"] = daily.find(tag).text

        result.append(flatten_daily_data)

    for food in root.findall("food"):
        flatten_pet_data = {}
        for tag in food_tags:
            if tag == 'vitamins':
                flatten_pet_data[tag + f".1"] = food.find(tag).find('a').text
                flatten_pet_data[tag + f".2"] = food.find(tag).find('c').text
            elif tag == 'serving':
                flatten_pet_data[tag + f".{food.find(tag).attrib['units']}"] = food.find(tag).text
            elif tag == 'calories':
                flatten_pet_data[tag + ".total"] = food.find(tag).attrib['total']
                flatten_pet_data[tag + ".fat"] = food.find(tag).attrib['fat']
            elif tag == 'minerals':
                flatten_pet_data[tag + f".1"] = food.find(tag).find('ca').text
                flatten_pet_data[tag + f".2"] = food.find(tag).find('fe').text
            else:
                flatten_pet_data[tag] = food.find(tag).text

        result.append(flatten_pet_data)

    return result

def flatten_pets_data(pets_data_path: str):
    with open(pets_data_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    result = []

    for pet in data["pets"]:
        flatten_pet_data = {}
        idx = 0
        for key, value in pet.items():
            if isinstance(value, list):
                for item in value:
                    flatten_pet_data[key + f".{idx}"] = item
                    idx += 1
            else:
                flatten_pet_data[key] = value

        result.append(flatten_pet_data)

    return result

if __name__ == "__main__":
    pprint(flatten_pets_data("pets-data.json"))
    pprint(flatten_nutrition_data("nutrition.xml"))