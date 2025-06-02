"""
Programador automático para ejecutar chequeos periódicos
Puede configurarse para ejecutar el sistema automáticamente
"""

import schedule
import time
import logging
import subprocess
import sys
from datetime import datetime
import json
import os

class AutoScheduler:
    def __init__(self, config_file='config.json'):
        """Inicializa el programador automático"""
        self.config_file = config_file
        self.setup_logging()
        
    def setup_logging(self):
        """Configura logging para el scheduler"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'logs/scheduler_{datetime.now().strftime("%Y%m%d")}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def run_monitoring_check(self):
        """Ejecuta el chequeo de monitoreo"""
        self.logger.info("Iniciando chequeo automático programado")
        
        try:
            # Ejecutar el script principal
            result = subprocess.run([
                sys.executable, 'run_system.py', 
                '--mode', 'check',
                '--config', self.config_file
            ], capture_output=True, text=True, timeout=300)  # 5 minutos timeout
            
            if result.returncode == 0:
                self.logger.info("Chequeo automático completado exitosamente")
                print(f"Chequeo completado - {datetime.now().strftime('%H:%M:%S')}")
            else:
                self.logger.error(f"Error en chequeo automático: {result.stderr}")
                print(f"Error en chequeo - {datetime.now().strftime('%H:%M:%S')}")
                
        except subprocess.TimeoutExpired:
            self.logger.error("Chequeo automático excedió el tiempo límite")
            print(f"Timeout en chequeo - {datetime.now().strftime('%H:%M:%S')}")
            
        except Exception as e:
            self.logger.error(f"Error inesperado en chequeo automático: {e}")
            print(f"Error inesperado - {datetime.now().strftime('%H:%M:%S')}")
    
    def start_scheduler(self):
        """Inicia el programador con horarios predefinidos"""
        self.logger.info("Iniciando programador automático")
        
        # Programar chequeos cada 2 horas durante el día
        schedule.every().day.at("08:00").do(self.run_monitoring_check)
        schedule.every().day.at("10:00").do(self.run_monitoring_check)
        schedule.every().day.at("12:00").do(self.run_monitoring_check)
        schedule.every().day.at("14:00").do(self.run_monitoring_check)
        schedule.every().day.at("16:00").do(self.run_monitoring_check)
        schedule.every().day.at("18:00").do(self.run_monitoring_check)
        schedule.every().day.at("20:00").do(self.run_monitoring_check)
        
        print("Programador iniciado. Horarios de chequeo:")
        print("   - 08:00, 10:00, 12:00, 14:00, 16:00, 18:00, 20:00")
        print("   - Presione Ctrl+C para detener")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Verificar cada minuto
                
        except KeyboardInterrupt:
            self.logger.info("Programador detenido por el usuario")
            print("\nProgramador detenido")

if __name__ == "__main__":
    scheduler = AutoScheduler()
    scheduler.start_scheduler()
