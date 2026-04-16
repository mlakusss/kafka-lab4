import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Настройка producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = 'user-actions'

# Возможные действия и товары
ACTIONS = ['view', 'cart', 'purchase']
PRODUCTS = {
    'laptop': 1000,
    'phone': 500,
    'tablet': 300,
    'headphones': 80,
    'mouse': 25,
    'keyboard': 45
}

def generate_event():
    """Генерирует одно событие пользователя"""
    user_id = random.randint(1, 100)
    action = random.choices(ACTIONS, weights=[0.7, 0.2, 0.1])[0]  # 70% view, 20% cart, 10% purchase
    product = random.choice(list(PRODUCTS.keys()))
    price = PRODUCTS[product] if action == 'purchase' else None

    event = {
        'user_id': user_id,
        'timestamp': datetime.now().isoformat(),
        'action': action,
        'product': product,
        'price': price
    }
    return event

def main():
    print(f"Начинаем отправку событий в топик '{TOPIC}'...")
    event_count = 0
    try:
        while True:
            event = generate_event()
            producer.send(TOPIC, value=event)
            event_count += 1
            if event_count % 10 == 0:
                print(f"Отправлено событий: {event_count}")
            time.sleep(random.uniform(0.1, 0.5))  # пауза для имитации потока
    except KeyboardInterrupt:
        print(f"\nОстановка producer. Всего отправлено: {event_count}")
    finally:
        producer.close()

if __name__ == '__main__':
    main()