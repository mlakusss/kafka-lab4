# v2
import json
import logging
import time
import threading
from collections import defaultdict, Counter
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from kafka import KafkaConsumer

# ----------------------------- Настройки ---------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'user-actions'
UPDATE_INTERVAL = 30  # секунд – для периодического обновления графиков (задача 8)
MESSAGES_BETWEEN_METRICS = 20  # каждые 20 сообщений – вывод метрик

# ----------------------------- Структуры данных (по заданию) -------------
user_behavior = defaultdict(list)          # история действий пользователя (все события)
session_data = defaultdict(dict)            # данные сессий (можно расширить)
hourly_activity = defaultdict(int)          # активность по часам
conversion_funnel = Counter()               # воронка: view, cart, purchase
revenue_data = []                           # список выручки с каждой покупки
product_performance = defaultdict(lambda: {
    'views': 0,
    'cart_adds': 0,
    'purchases': 0,
    'revenue': 0
})

# Для отслеживания сессий (упрощённо: считаем, что сессия – это набор действий одного пользователя
# с перерывом не более 30 минут, но для простоты сделаем сессией все действия пользователя за время работы)
user_session_actions = defaultdict(list)    # список действий пользователя с timestamps

# Для ML (простая модель: вероятность покупки на основе числа просмотров и добавлений в корзину)
# Обучим на лету? Для демо используем заранее подготовленную логистическую регрессию (имитация).
# В реальности можно обучить модель на накопленных данных.
# Здесь для простоты сделаем эвристическую функцию.

# ----------------------------- Вспомогательные функции -------------------
def update_structures(event):
    """Обновляет все структуры на основе одного события"""
    user_id = event['user_id']
    action = event['action']
    product = event['product']
    price = event.get('price', 0)
    timestamp = datetime.fromisoformat(event['timestamp'])
    hour = timestamp.hour

    # user_behavior – просто список действий
    user_behavior[user_id].append(event)

    # hourly_activity
    hourly_activity[hour] += 1

    # conversion_funnel
    conversion_funnel[action] += 1

    # revenue_data
    if action == 'purchase' and price:
        revenue_data.append(price)

    # product_performance
    perf = product_performance[product]
    if action == 'view':
        perf['views'] += 1
    elif action == 'cart':
        perf['cart_adds'] += 1
    elif action == 'purchase':
        perf['purchases'] += 1
        perf['revenue'] += price

    # Для сессий (упрощённо: все действия пользователя)
    user_session_actions[user_id].append((timestamp, action, product, price))

# ----------------------------- Расчёт метрик (задача 5) -------------------
def calculate_conversion_rates():
    """Конверсии view -> cart, cart -> purchase, view -> purchase"""
    views = conversion_funnel.get('view', 0)
    carts = conversion_funnel.get('cart', 0)
    purchases = conversion_funnel.get('purchase', 0)

    view_to_cart = (carts / views) if views > 0 else 0
    cart_to_purchase = (purchases / carts) if carts > 0 else 0
    view_to_purchase = (purchases / views) if views > 0 else 0
    return {
        'view_to_cart': round(view_to_cart, 3),
        'cart_to_purchase': round(cart_to_purchase, 3),
        'view_to_purchase': round(view_to_purchase, 3)
    }

def calculate_average_session_value():
    """Средний чек сессии (на основе revenue_data)"""
    if not revenue_data:
        return 0
    return round(np.mean(revenue_data), 2)

def find_top_customers():
    """Топ-5 пользователей по числу действий"""
    user_activity_count = {uid: len(actions) for uid, actions in user_behavior.items()}
    top5 = sorted(user_activity_count.items(), key=lambda x: x[1], reverse=True)[:5]
    return top5

def detect_abandonment_sessions():
    """Сессии с добавлением в корзину, но без покупки"""
    abandoned = []
    for uid, actions in user_session_actions.items():
        has_cart = any(a[1] == 'cart' for a in actions)
        has_purchase = any(a[1] == 'purchase' for a in actions)
        if has_cart and not has_purchase:
            abandoned.append(uid)
    return len(abandoned), abandoned[:5]  # количество и примеры

def check_for_alerts(recent_events):
    """
    Проверка аномалий:
    1. >50% logout (у нас нет logout, заменим на >50% purchase? но условие другое)
       Сделаем: если доля purchase > 30% – аномалия (слишком много покупок)
    2. Высокий уровень отказов от корзины (cart без purchase) – уже есть detect_abandonment_sessions
    3. Один пользователь делает >10 действий за минуту
    4. Нет покупок в последних 50 событиях
    """
    alerts = []
    # 1. Доля покупок в последних событиях
    if recent_events:
        purchases = sum(1 for e in recent_events if e['action'] == 'purchase')
        ratio = purchases / len(recent_events)
        if ratio > 0.3:
            alerts.append(f"Высокая доля покупок: {ratio:.2%}")

    # 2. Отказы от корзины (глобально)
    abandoned_count, _ = detect_abandonment_sessions()
    if abandoned_count > 10:
        alerts.append(f"Много брошенных корзин: {abandoned_count} пользователей")

    # 3. Один пользователь >10 действий за минуту (анализируем последние события)
    if recent_events:
        now = datetime.now()
        minute_ago = now.timestamp() - 60
        user_counts = defaultdict(int)
        for e in recent_events:
            ts = datetime.fromisoformat(e['timestamp']).timestamp()
            if ts > minute_ago:
                user_counts[e['user_id']] += 1
        for uid, cnt in user_counts.items():
            if cnt > 10:
                alerts.append(f"Пользователь {uid} совершил {cnt} действий за минуту")
                break

    # 4. Нет покупок в последних 50 событиях
    if len(recent_events) >= 50:
        purchases_last50 = sum(1 for e in recent_events[-50:] if e['action'] == 'purchase')
        if purchases_last50 == 0:
            alerts.append("Нет покупок в последних 50 событиях")

    return alerts

# ----------------------------- Сохранение данных (задача 5) --------------
def export_session_data_to_csv():
    """Сохраняет данные о сессиях в CSV (user_id, количество действий, сумма выручки)"""
    session_records = []
    for uid, actions in user_session_actions.items():
        total_revenue = sum(a[3] for a in actions if a[1] == 'purchase')
        session_records.append({
            'user_id': uid,
            'action_count': len(actions),
            'total_revenue': total_revenue,
            'has_purchase': any(a[1] == 'purchase' for a in actions)
        })
    df = pd.DataFrame(session_records)
    df.to_csv('user_sessions.csv', index=False)
    logging.info("Сохранено user_sessions.csv")
    return df

def save_real_time_metrics():
    """Сохраняет текущие метрики в JSON"""
    metrics = {
        'timestamp': datetime.now().isoformat(),
        'total_events': sum(len(v) for v in user_behavior.values()),
        'unique_users': len(user_behavior),
        'conversion_rates': calculate_conversion_rates(),
        'avg_session_value': calculate_average_session_value(),
        'top_customers': find_top_customers(),
        'abandoned_sessions_count': detect_abandonment_sessions()[0],
        'total_revenue': sum(revenue_data),
        'hourly_activity': dict(hourly_activity)
    }
    with open('metrics_snapshot.json', 'w') as f:
        json.dump(metrics, f, indent=2)
    logging.info("Сохранено metrics_snapshot.json")
    return metrics

# ----------------------------- Визуализации (задача 6) -------------------
def create_dashboard():
    """Создаёт и сохраняет дашборд из 6 графиков"""
    plt.style.use('seaborn-v0_8-darkgrid')
    fig = plt.figure(figsize=(16, 12))

    # 1. Воронка конверсий
    ax1 = plt.subplot(2, 3, 1)
    stages = ['Просмотры', 'Корзина', 'Покупки']
    values = [conversion_funnel.get('view', 0),
              conversion_funnel.get('cart', 0),
              conversion_funnel.get('purchase', 0)]
    colors = ['#3498db', '#f39c12', '#2ecc71']
    bars = ax1.bar(stages, values, color=colors)
    ax1.set_title('Воронка конверсий', fontsize=12)
    ax1.set_ylabel('Количество действий')
    for bar, val in zip(bars, values):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, str(val),
                 ha='center', va='bottom')

    # 2. Топ продуктов по выручке
    ax2 = plt.subplot(2, 3, 2)
    products = list(product_performance.keys())
    revenues = [product_performance[p]['revenue'] for p in products]
    sorted_idx = np.argsort(revenues)[::-1]
    products_sorted = [products[i] for i in sorted_idx]
    revenues_sorted = [revenues[i] for i in sorted_idx]
    ax2.bar(products_sorted[:5], revenues_sorted[:5], color='#e67e22')
    ax2.set_title('Топ-5 продуктов по выручке', fontsize=12)
    ax2.set_xlabel('Продукт')
    ax2.set_ylabel('Выручка, $')
    ax2.tick_params(axis='x', rotation=45)

    # 3. Активность пользователей (пузырьковая диаграмма: user_id vs кол-во действий, размер = выручка)
    ax3 = plt.subplot(2, 3, 3)
    user_ids = []
    actions_cnt = []
    user_revenue = []
    for uid, actions in user_behavior.items():
        user_ids.append(uid)
        actions_cnt.append(len(actions))
        rev = sum(e.get('price', 0) for e in actions if e.get('action') == 'purchase')
        user_revenue.append(rev)
    # Нормализуем размер для отображения (от 20 до 500)
    sizes = [20 + (rev / max(user_revenue) * 480) if max(user_revenue) > 0 else 20 for rev in user_revenue]
    scatter = ax3.scatter(user_ids, actions_cnt, s=sizes, alpha=0.6, c='#1abc9c')
    ax3.set_title('Активность пользователей (размер пузырька = выручка)', fontsize=10)
    ax3.set_xlabel('User ID')
    ax3.set_ylabel('Количество действий')

    # 4. Распределение длины сессии (гистограмма)
    ax4 = plt.subplot(2, 3, 4)
    session_lengths = [len(actions) for actions in user_session_actions.values()]
    ax4.hist(session_lengths, bins=15, color='#9b59b6', edgecolor='black')
    ax4.set_title('Распределение действий в сессии', fontsize=12)
    ax4.set_xlabel('Количество действий в сессии')
    ax4.set_ylabel('Частота')

    # 5. Активность по часам (линейный график)
    ax5 = plt.subplot(2, 3, 5)
    hours = sorted(hourly_activity.keys())
    counts = [hourly_activity[h] for h in hours]
    ax5.plot(hours, counts, marker='o', linestyle='-', color='#e74c3c')
    ax5.set_title('Активность по часам', fontsize=12)
    ax5.set_xlabel('Час')
    ax5.set_ylabel('Количество событий')
    ax5.grid(True)

    # 6. Соотношение типов действий (круговая диаграмма)
    ax6 = plt.subplot(2, 3, 6)
    action_labels = ['Просмотры', 'Корзина', 'Покупки']
    action_values = [conversion_funnel.get('view', 0),
                     conversion_funnel.get('cart', 0),
                     conversion_funnel.get('purchase', 0)]
    ax6.pie(action_values, labels=action_labels, autopct='%1.1f%%',
            colors=['#3498db', '#f39c12', '#2ecc71'], startangle=90)
    ax6.set_title('Доля действий пользователей', fontsize=12)

    plt.tight_layout()
    plt.savefig('kafka_analytics_dashboard.png', dpi=150)
    logging.info("Дашборд сохранён как kafka_analytics_dashboard.png")
    plt.close(fig)

# ----------------------------- Текстовый отчёт (задача 6) ----------------
def generate_text_report():
    """Формирует красивый отчёт и сохраняет в файл"""
    total_events = sum(len(v) for v in user_behavior.values())
    unique_users = len(user_behavior)
    conv_rates = calculate_conversion_rates()
    total_revenue = sum(revenue_data)
    top_product = max(product_performance.items(), key=lambda x: x[1]['revenue'])
    alerts = check_for_alerts([])  # используем последние события? упростим

    report = f"""
    ========== ОТЧЁТ ПО ПОТОКОВЫМ ДАННЫМ ==========
    Обработано событий: {total_events}
    Уникальных пользователей: {unique_users}
    Конверсия покупок: {conv_rates['view_to_purchase']*100:.1f}%
    Общая выручка: ${total_revenue:,.2f}
    Топ продукт: {top_product[0]} (${top_product[1]['revenue']:,.2f})
    Оповещения: {len(alerts)} подозрительных сессий
    -----------------------------------------------
    """
    print(report)

    filename = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(filename, 'w') as f:
        f.write(report)
    logging.info(f"Отчёт сохранён в {filename}")
    return report

# ----------------------------- ML модель (задача 8) ----------------------
class SimplePurchasePredictor:
    """
    Простая модель: на основе количества просмотров и добавлений в корзину
    предсказывает вероятность покупки. Для демонстрации используем логистическую регрессию,
    обученную на сгенерированных данных. Здесь для простоты – эвристика.
    """
    def __init__(self):
        # Коэффициенты (условно обучены)
        self.coef_view = 0.05
        self.coef_cart = 0.3
        self.intercept = -1.5

    def predict_proba(self, views, carts):
        """Возвращает вероятность покупки"""
        logit = self.intercept + self.coef_view * views + self.coef_cart * carts
        prob = 1 / (1 + np.exp(-logit))
        return min(max(prob, 0), 1)

    def predict_for_user(self, user_id):
        """Предсказание для конкретного пользователя по его истории"""
        actions = user_behavior.get(user_id, [])
        views = sum(1 for a in actions if a['action'] == 'view')
        carts = sum(1 for a in actions if a['action'] == 'cart')
        prob = self.predict_proba(views, carts)
        return prob

# Глобальный экземпляр модели
predictor = SimplePurchasePredictor()

# ----------------------------- Периодическое обновление графиков (задача 8) --
def periodic_dashboard(interval_sec=30):
    """Фоновый поток для сохранения дашборда каждые interval_sec секунд"""
    while True:
        time.sleep(interval_sec)
        if user_behavior:  # если есть данные
            create_dashboard()
            export_session_data_to_csv()
            save_real_time_metrics()
            generate_text_report()

# ----------------------------- Основной consumer -------------------------
def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    logging.info(f"Consumer запущен, слушаем топик '{KAFKA_TOPIC}'")

    # Запуск фонового потока для периодического обновления дашборда (каждые 30 секунд)
    dashboard_thread = threading.Thread(target=periodic_dashboard, args=(UPDATE_INTERVAL,), daemon=True)
    dashboard_thread.start()

    message_count = 0
    recent_events = []  # для детектора аномалий (последние 100 событий)
    try:
        for message in consumer:
            event = message.value
            update_structures(event)
            message_count += 1
            recent_events.append(event)
            if len(recent_events) > 100:
                recent_events.pop(0)

            # Каждые MESSAGES_BETWEEN_METRICS сообщений выводим метрики и проверяем аномалии
            if message_count % MESSAGES_BETWEEN_METRICS == 0:
                logging.info(f"\n--- Обработано сообщений: {message_count} ---")
                conv = calculate_conversion_rates()
                logging.info(f"Конверсии: {conv}")
                avg_val = calculate_average_session_value()
                logging.info(f"Средний чек: ${avg_val}")
                top5 = find_top_customers()
                logging.info(f"Топ-5 пользователей: {top5}")
                ab_count, _ = detect_abandonment_sessions()
                logging.info(f"Брошенные корзины: {ab_count}")
                alerts = check_for_alerts(recent_events)
                if alerts:
                    logging.warning(f"Оповещения: {alerts}")

                # Демонстрация ML: предсказание для нескольких пользователей
                sample_users = list(user_behavior.keys())[:3]
                for uid in sample_users:
                    prob = predictor.predict_for_user(uid)
                    logging.info(f"ML: вероятность покупки для user {uid} = {prob:.2f}")

                # Сохраняем метрики и CSV (можно и чаще, но для экономии каждые 20 сообщений)
                save_real_time_metrics()
                export_session_data_to_csv()
                generate_text_report()
                # Дашборд сохраняется фоном раз в 30 секунд, но можно и принудительно
                create_dashboard()

    except KeyboardInterrupt:
        logging.info("Остановка consumer по Ctrl+C")
    finally:
        consumer.close()
        # Финальное сохранение всех данных
        create_dashboard()
        export_session_data_to_csv()
        save_real_time_metrics()
        generate_text_report()
        logging.info("Consumer завершён.")

if __name__ == '__main__':
    main()