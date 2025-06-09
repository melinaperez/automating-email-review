#!/usr/bin/env python3
"""
Resolución AM/PM basada en el contenido de los archivos de presión
"""

import os
import pandas as pd
from datetime import datetime, timedelta
import logging
import re

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ContentBasedAMPMResolver:
    def __init__(self):
        """Inicializa el resolvedor de AM/PM basado en contenido"""
        self.data_dir = "data"
        self.patient_pressure_data = {}  # Cache de datos de presión por paciente
    
    def find_best_pressure_file(self, patient_dir: str) -> str:
        """
        Encuentra el archivo de presión más adecuado (más nuevo y más pesado)
        ACTUALIZADO: Con logging mejorado y criterios más estrictos
        
        Args:
            patient_dir: Ruta al directorio del paciente
        
        Returns:
            Ruta al mejor archivo de presión o None si no hay archivos
        """
        patient_path = os.path.join(self.data_dir, patient_dir)
        
        if not os.path.exists(patient_path):
            logger.warning(f"Directorio no encontrado: {patient_path}")
            return None
        
        # Buscar todos los archivos de presión con criterios más estrictos
        pressure_files = []
        all_files = os.listdir(patient_path)
        
        logger.info(f"🔍 Buscando archivos de presión en {patient_dir}...")
        logger.info(f"📂 Archivos totales en directorio: {len(all_files)}")
        
        for file in all_files:
            file_path = os.path.join(patient_path, file)
            if os.path.isfile(file_path):
                # Criterios más estrictos para archivos de presión
                is_csv = file.lower().endswith('.csv')
                has_pressure = 'pressure' in file.lower()
                
                logger.debug(f"   📄 {file}: CSV={is_csv}, Pressure={has_pressure}")
                
                if is_csv or has_pressure:
                    try:
                        # Obtener tamaño y fecha de modificación
                        size = os.path.getsize(file_path)
                        mtime = os.path.getmtime(file_path)
                        
                        # Verificar que el archivo no esté vacío
                        if size > 0:
                            pressure_files.append((file_path, size, mtime))
                            logger.info(f"   ✅ {file}: {size} bytes, {datetime.fromtimestamp(mtime)}")
                        else:
                            logger.warning(f"   ❌ {file}: archivo vacío")
                            
                    except Exception as e:
                        logger.warning(f"   ❌ Error evaluando {file}: {e}")
                        continue
        
        logger.info(f"📊 Archivos de presión válidos encontrados: {len(pressure_files)}")
        
        if not pressure_files:
            logger.warning(f"❌ No se encontraron archivos de presión válidos en {patient_path}")
            return None
        
        # Si solo hay un archivo, devolverlo directamente
        if len(pressure_files) == 1:
            best_file = pressure_files[0][0]
            logger.info(f"📄 Un solo archivo de presión: {os.path.basename(best_file)}")
            return best_file
        
        # Ordenar por tamaño (descendente) y fecha (más reciente primero)
        pressure_files.sort(key=lambda x: (-x[1], -x[2]))
        
        best_file = pressure_files[0][0]
        best_size = pressure_files[0][1]
        best_mtime = pressure_files[0][2]
        
        logger.info(f"🏆 MEJOR archivo de presión seleccionado:")
        logger.info(f"   📄 Archivo: {os.path.basename(best_file)}")
        logger.info(f"   📊 Tamaño: {best_size} bytes")
        logger.info(f"   📅 Modificado: {datetime.fromtimestamp(best_mtime)}")
        
        # Mostrar archivos que se ignoran
        ignored_files = pressure_files[1:]
        if ignored_files:
            logger.info(f"❌ Archivos de presión IGNORADOS ({len(ignored_files)}):")
            for ignored_file, ignored_size, ignored_mtime in ignored_files:
                logger.info(f"   - {os.path.basename(ignored_file)} ({ignored_size} bytes)")
        
        return best_file
    
    def extract_pressure_times(self, pressure_file: str) -> list:
        """
        Extrae todas las fechas/horas del archivo de presión
        
        Args:
            pressure_file: Ruta al archivo CSV de presión
        
        Returns:
            Lista de datetimes extraídos del archivo
        """
        times = []
        
        try:
            # Intentar con diferentes encodings
            for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                try:
                    df = pd.read_csv(pressure_file, encoding=encoding)
                    logger.info(f"CSV leído con encoding {encoding}: {pressure_file}")
                    break
                except UnicodeDecodeError:
                    continue
            else:
                logger.error(f"No se pudo leer el archivo con ninguna codificación: {pressure_file}")
                return times
            
            # Buscar columna de fecha
            date_columns = [col for col in df.columns 
                           if any(word in col.lower() for word in ['fecha', 'date', 'time', 'timestamp', 'medición'])]
            
            if not date_columns:
                logger.warning(f"No se encontró columna de fecha en {pressure_file}")
                logger.info(f"Columnas disponibles: {list(df.columns)}")
                return times
            
            date_col = date_columns[0]
            logger.info(f"Usando columna de fecha: {date_col}")
            
            # Extraer y parsear todas las fechas
            for _, row in df.iterrows():
                try:
                    date_str = str(row[date_col])
                    
                    # Intentar parsear con pandas
                    parsed_date = pd.to_datetime(date_str)
                    if pd.notna(parsed_date):
                        times.append(parsed_date.to_pydatetime())
                        continue
                    
                    # Intentar formatos comunes
                    formats = [
                        '%Y/%m/%d %H:%M',
                        '%Y-%m-%d %H:%M:%S',
                        '%d/%m/%Y %H:%M',
                        '%m/%d/%Y %H:%M',
                        '%Y/%m/%d %H:%M:%S'
                    ]
                    
                    for fmt in formats:
                        try:
                            dt = datetime.strptime(date_str, fmt)
                            times.append(dt)
                            break
                        except ValueError:
                            continue
                    
                except Exception as e:
                    logger.debug(f"Error parseando fecha '{date_str}': {e}")
                    continue
            
            logger.info(f"Extraídas {len(times)} fechas del archivo {os.path.basename(pressure_file)}")
            
            # Mostrar las primeras fechas extraídas para debugging
            if times:
                logger.info(f"Primeras fechas extraídas: {times[:5]}")
            
            return times
            
        except Exception as e:
            logger.error(f"Error extrayendo fechas de {pressure_file}: {e}")
            return times
    
    def get_patient_pressure_times(self, patient_dir: str) -> list:
        """
        Obtiene todas las fechas de presión para un paciente
        
        Args:
            patient_dir: Nombre del directorio del paciente
        
        Returns:
            Lista de datetimes de mediciones de presión
        """
        # Verificar si ya tenemos los datos en caché
        if patient_dir in self.patient_pressure_data:
            return self.patient_pressure_data[patient_dir]
        
        # Encontrar el mejor archivo de presión
        best_file = self.find_best_pressure_file(patient_dir)
        if not best_file:
            return []
        
        # Extraer fechas del archivo
        pressure_times = self.extract_pressure_times(best_file)
        
        # Guardar en caché
        self.patient_pressure_data[patient_dir] = pressure_times
        
        return pressure_times
    
    def resolve_ecg_ambiguity(self, ecg_datetime: datetime, patient_dir: str) -> datetime:
        """
        Resuelve la ambigüedad AM/PM de un ECG usando datos de presión
        
        Args:
            ecg_datetime: Datetime del ECG con posible ambigüedad
            patient_dir: Directorio del paciente
        
        Returns:
            Datetime con ambigüedad resuelta
        """
        # Si no es hora ambigua (1-12), devolver tal como está
        hour = ecg_datetime.hour
        if not (1 <= hour <= 12):
            return ecg_datetime
        
        logger.info(f"Resolviendo ambigüedad para ECG: {ecg_datetime}")
        
        # Obtener fechas de presión
        pressure_times = self.get_patient_pressure_times(patient_dir)
        
        if not pressure_times:
            logger.warning(f"No hay datos de presión para {patient_dir}, usando heurística")
            return self._resolve_with_heuristics(ecg_datetime)
        
        # Crear versiones AM y PM del datetime
        ecg_am = ecg_datetime
        ecg_pm = ecg_datetime.replace(hour=hour + 12) if hour < 12 else ecg_datetime
        
        # Buscar la medición de presión más cercana
        min_diff_am = float('inf')
        min_diff_pm = float('inf')
        
        for pressure_dt in pressure_times:
            # Calcular diferencia en minutos
            diff_am = abs((pressure_dt - ecg_am).total_seconds() / 60)
            diff_pm = abs((pressure_dt - ecg_pm).total_seconds() / 60)
            
            if diff_am < min_diff_am:
                min_diff_am = diff_am
            
            if diff_pm < min_diff_pm:
                min_diff_pm = diff_pm
        
        logger.info(f"Diferencia mínima: AM={min_diff_am:.1f}min, PM={min_diff_pm:.1f}min")
        
        # Decidir basado en la menor diferencia (dentro de 120 minutos = 2 horas)
        if min_diff_am <= 120 and min_diff_am < min_diff_pm:
            logger.info(f"Resuelto como AM: {ecg_am}")
            return ecg_am
        elif min_diff_pm <= 120 and min_diff_pm < min_diff_am:
            logger.info(f"Resuelto como PM: {ecg_pm}")
            return ecg_pm
        else:
            # Si no hay una diferencia clara, usar heurística
            logger.info("No hay diferencia clara, usando heurística")
            return self._resolve_with_heuristics(ecg_datetime)
    
    def _resolve_with_heuristics(self, ecg_datetime: datetime) -> datetime:
        """
        Resuelve ambigüedad usando heurísticas cuando no hay datos de presión
        
        Args:
            ecg_datetime: Datetime con ambigüedad
        
        Returns:
            Datetime resuelto
        """
        hour = ecg_datetime.hour
        
        # Heurísticas basadas en patrones médicos típicos
        if 1 <= hour <= 5:
            # Horas muy tempranas (1-5) probablemente son AM
            logger.info(f"Hora {hour} clasificada como AM (muy temprano)")
            return ecg_datetime
        elif 6 <= hour <= 11:
            # Horas de mañana (6-11) probablemente son AM
            logger.info(f"Hora {hour} clasificada como AM (horario matutino típico)")
            return ecg_datetime
        elif hour == 12:
            # Mediodía (12) - más probable que sea PM
            logger.info(f"Hora 12 clasificada como PM (mediodía)")
            return ecg_datetime
        else:
            # No debería llegar aquí, pero por si acaso
            return ecg_datetime

def test_resolver():
    """Prueba el resolvedor con algunos casos"""
    resolver = ContentBasedAMPMResolver()
    
    # Probar con algunos pacientes
    patient_dirs = os.listdir(resolver.data_dir)
    
    for patient_dir in patient_dirs[:3]:  # Probar con los primeros 3 pacientes
        print(f"\n=== Probando con paciente: {patient_dir} ===")
        
        # Encontrar el mejor archivo de presión
        best_file = resolver.find_best_pressure_file(patient_dir)
        if best_file:
            print(f"Mejor archivo de presión: {os.path.basename(best_file)}")
            
            # Extraer fechas
            pressure_times = resolver.extract_pressure_times(best_file)
            print(f"Fechas extraídas: {len(pressure_times)}")
            
            if pressure_times:
                print(f"Ejemplos de fechas: {pressure_times[:3]}")
                
                # Probar resolución con algunas horas ambiguas
                test_hours = [8, 10, 2, 4, 7]
                for hour in test_hours:
                    test_dt = datetime.now().replace(hour=hour, minute=30)
                    resolved = resolver.resolve_ecg_ambiguity(test_dt, patient_dir)
                    print(f"Hora {hour}:30 -> Resuelta como {resolved.hour}:{resolved.minute}")
        else:
            print(f"No se encontró archivo de presión para {patient_dir}")

if __name__ == "__main__":
    test_resolver()
