import requests
from datetime import datetime, timedelta

def get_market_volume(market_id, timestamp=None):
    """
    Obtiene el volumen de transacciones para el mercado dado en un momento específico.
    """
    url = f'https://www.buda.com/api/v2/markets/{market_id}/volume'
    if timestamp:
        url += f'?timestamp={timestamp}'
    response = requests.get(url)
    return response.json()['volume']

def get_average_price(market_id, start_time, end_time):
    """
    Obtiene el precio promedio de las transacciones de venta en el mercado dado entre dos momentos específicos.
    """
    url = f'https://www.buda.com/api/v2/markets/{market_id}/trades'
    params = {'since': start_time, 'until': end_time}
    response = requests.get(url, params=params)
    trades = response.json()['trades']['entries']
    total_volume = sum(float(trade[1]) for trade in trades if trade[3] == 'sell')
    total_amount = sum(float(trade[1]) * float(trade[2]) for trade in trades if trade[3] == 'sell')
    return total_amount / total_volume

def calculate_money_not_gained(money_transacted_clp, commission_rate):
    """
    Calcula el dinero no ganado debido a la liberación de comisiones.
    """
    return money_transacted_clp * commission_rate

def get_previous_year_volume(market_id, event_date):
    """
    Obtiene el volumen de transacciones para el mismo día del año anterior.
    """
    one_year_before = event_date - timedelta(days=365)
    timestamp_one_year_before = int(one_year_before.timestamp())
    return get_market_volume(market_id, timestamp_one_year_before)['bid_volume_24h'][0]

if __name__ == "__main__":
    market_id = 'btc-clp'
    
    # Fecha y hora del evento Black Buda
    event_date = datetime.strptime('2024-03-01', '%Y-%m-%d')
    start_time = int(event_date.replace(hour=12).timestamp())
    end_time = int(event_date.replace(hour=13).timestamp())
    
    # Obtener el volumen de transacciones durante el evento
    volume_data = get_market_volume(market_id)
    event_volume_btc = float(volume_data['bid_volume_24h'][0])
    
    # Calcular el precio promedio durante el evento
    average_price = get_average_price(market_id, start_time, end_time)
    
    # Calcular el dinero transado en CLP
    money_transacted_clp = event_volume_btc * average_price
    
    # Obtener el volumen de transacciones del mismo día del año anterior
    previous_year_volume_btc = float(get_previous_year_volume(market_id, event_date))
    
    # Calcular el aumento porcentual en el volumen de transacciones
    percentage_increase = ((event_volume_btc - previous_year_volume_btc) / previous_year_volume_btc) * 100
    
    # Calcular el dinero dejado de ganar
    commission_rate = 0.008  # 0.8%
    money_not_gained_clp = calculate_money_not_gained(money_transacted_clp, commission_rate)
    
    # Imprimir las respuestas con preguntas simplificadas
    # ¿Cuánto dinero (en CLP) se transó durante el evento "Black Buda" BTC-CLP? (truncar en 2 decimales)
    print(f"Dinero transado (CLP): {money_transacted_clp:.2f}")
    
    # ¿Cuál fue el aumento porcentual en el volumen de transacciones (en BTC) comparado con el mismo día del año anterior? (truncar en 2 decimales)
    print(f"Aumento porcentual del volumen (BTC): {percentage_increase:.2f}%")
    
    # ¿Cuánto dinero (en CLP) se dejó de ganar debido a la liberación de comisiones durante el BlackBuda? (truncar en 2 decimales)
    print(f"Dinero dejado de ganar (CLP): {money_not_gained_clp:.2f}")
    
    # Reflexión sobre problemas y mejoras
    """
    Durante eventos especiales como BlackBuda, pueden surgir problemas como sobrecarga de servidores, 
    errores en la ejecución de transacciones, y dificultades para manejar el volumen incrementado de usuarios. 
    Para priorizar la corrección o mejora, se debe enfocar en:
    
    1. Escalabilidad del servidor: Asegurarse de que los servidores puedan manejar incrementos significativos de tráfico.
    2. Robustez del sistema de transacciones: Garantizar que todas las transacciones se ejecuten sin errores.
    3. Usar Celery con workers: Para procesar tareas en segundo plano y manejar múltiples solicitudes concurrentes.
    4. Implementar un sistema de monitoreo: Para detectar problemas en tiempo real y tomar medidas correctivas.
    5. Usar kafka para procesar eventos: Para manejar eventos en tiempo real y garantizar la integridad de los datos.
    6. Comunicación al usuario: Mantener a los usuarios informados en caso de cualquier problema técnico.
    """
