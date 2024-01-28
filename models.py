import datetime
"""
Data model of Bama listener
"""

class EventData:
    def __init__(self, event):
        try:
            self.code = event['detail']['code']
            self.car_name = event['detail']['title'].split('، ')[1].strip()
            self.car_brand = event['detail']['title'].split('، ')[0].strip()
            self.year = event['detail']['year'].strip()
            self.time = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            self.price = str(event['price']['price'].replace(',', ''))
            self.mileage = str(0 if 'کارکرده' in event['detail']['mileage'] or 'صفر' in event['detail']['mileage'] else int(event['detail']['mileage'].split(' ')[0].replace(',', '')))
        except Exception as e:
            print(e)
            print(event)

    def to_dict(self):
        return {
            "code": self.code,
            "car_name": self.car_name,
            "car_brand": self.car_brand,
            "year": self.year,
            "time": self.time,
            "price": self.price,
            "mileage": self.mileage
        }
