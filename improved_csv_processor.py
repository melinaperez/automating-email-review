#!/usr/bin/env python3
"""
Procesador mejorado de archivos CSV que selecciona el mejor archivo por paciente
y extrae múltiples mediciones clasificadas por franjas horarias
"""

import os
import pandas as pd
from datetime import datetime, time
import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

class ImprovedCSVProcessor:
    def __init__(self):
        """Inicializa el procesador de CSV mejorado"""
        self.data_dir = "data"
        
        # Definir franjas horarias
        self.time_slots = {
            'matutina': (time(4, 0), time(12, 59)),    # 04:00 - 12:59
            'vespertina': (time(13, 0), time(3, 59))   # 13:00 - 03:59 (del día siguiente)
        }
        
        # Rangos normales para validación
        self.pressure_ranges = {
            'systolic': (70, 250),
            'diastolic': (40, 150),
            'pulse': (40, 150)
        }
    
    def find_best_csv_file(self, patient_dir: str) -> Optional[str]:
        """
        Encuentra el archivo CSV más reciente y pesado para un paciente
        
        Args:
            patient_dir: Nombre del directorio del paciente
        
        Returns:
            Ruta al mejor archivo CSV o None si no hay archivos
        """
        patient_path = os.path.join(self.data_dir, patient_dir)
        
        if not os.path.exists(patient_path):
            logger.warning(f"Directorio no encontrado: {patient_path}")
            return None
        
        # Buscar todos los archivos CSV
        csv_files = []
        all_files = os.listdir(patient_path)
        
        logger.info(f"🔍 Buscando archivos CSV para {patient_dir}...")
        
        for file in all_files:
            file_path = os.path.join(patient_path, file)
            if os.path.isfile(file_path):
                # Criterios para archivos de presión
                is_csv = file.lower().endswith('.csv')
                has_pressure = 'pressure' in file.lower()
                
                if is_csv or has_pressure:
                    try:
                        # Obtener tamaño y fecha de modificación
                        size = os.path.getsize(file_path)
                        mtime = os.path.getmtime(file_path)
                        
                        # Verificar que el archivo no esté vacío
                        if size > 0:
                            csv_files.append((file_path, size, mtime))
                            logger.info(f"   📄 {file}: {size} bytes, {datetime.fromtimestamp(mtime)}")
                        else:
                            logger.warning(f"   ❌ {file}: archivo vacío")
                            
                    except Exception as e:
                        logger.warning(f"   ❌ Error evaluando {file}: {e}")
                        continue
        
        if not csv_files:
            logger.warning(f"❌ No se encontraron archivos CSV válidos para {patient_dir}")
            return None
        
        # Si solo hay un archivo, devolverlo directamente
        if len(csv_files) == 1:
            best_file = csv_files[0][0]
            logger.info(f"📄 Un solo archivo CSV: {os.path.basename(best_file)}")
            return best_file
        
        # Ordenar por tamaño (descendente) y fecha (más reciente primero)
        csv_files.sort(key=lambda x: (-x[1], -x[2]))
        
        best_file = csv_files[0][0]
        best_size = csv_files[0][1]
        best_mtime = csv_files[0][2]
        
        logger.info(f"🏆 MEJOR archivo CSV seleccionado para {patient_dir}:")
        logger.info(f"   📄 Archivo: {os.path.basename(best_file)}")
        logger.info(f"   📊 Tamaño: {best_size} bytes")
        logger.info(f"   📅 Modificado: {datetime.fromtimestamp(best_mtime)}")
        
        # Mostrar archivos que se ignoran
        ignored_files = csv_files[1:]
        if ignored_files:
            logger.info(f"❌ Archivos CSV IGNORADOS para {patient_dir} ({len(ignored_files)}):")
            for ignored_file, ignored_size, ignored_mtime in ignored_files:
                logger.info(f"   - {os.path.basename(ignored_file)} ({ignored_size} bytes)")
        
        return best_file
    
    def extract_all_pressure_measurements(self, csv_file: str) -> List[Dict]:
        """
        Extrae TODAS las mediciones de presión del archivo CSV y las clasifica por franjas
        
        Args:
            csv_file: Ruta al archivo CSV
        
        Returns:
            Lista de mediciones clasificadas por franja horaria
        """
        measurements = []
        
        try:
            # Leer CSV con diferentes encodings
            df = None
            for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                try:
                    df = pd.read_csv(csv_file, encoding=encoding)
                    logger.info(f"CSV leído con encoding {encoding}: {os.path.basename(csv_file)}")
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                logger.error(f"No se pudo leer el archivo: {csv_file}")
                return measurements
            
            logger.info(f"📊 CSV cargado: {len(df)} filas, columnas: {list(df.columns)}")
            
            # Detectar columnas relevantes
            columns = self.detect_csv_columns(df)
            if not columns:
                logger.error(f"No se encontraron columnas válidas en {csv_file}")
                return measurements
            
            # Procesar cada fila como una medición independiente
            for index, row in df.iterrows():
                try:
                    # Extraer datos de presión de esta fila
                    pressure_data = {}
                    for data_type, col_name in columns.items():
                        if col_name in df.columns and data_type in ['systolic', 'diastolic', 'pulse']:
                            try:
                                value = row[col_name]
                                if pd.notna(value):
                                    pressure_data[data_type] = float(value)
                            except (ValueError, TypeError):
                                # Intentar extraer números del texto
                                text_value = str(row[col_name])
                                numbers = re.findall(r'\d+', text_value)
                                if numbers:
                                    pressure_data[data_type] = float(numbers[0])
                    
                    # Verificar que tenemos al menos presión sistólica y diastólica
                    if 'systolic' not in pressure_data or 'diastolic' not in pressure_data:
                        continue
                    
                    # Extraer fecha/hora de esta medición
                    measurement_time = self.extract_measurement_time(row, columns)
                    if not measurement_time:
                        continue
                    
                    # Clasificar en franja horaria
                    time_slot = self.classify_time_slot(measurement_time)
                    
                    # Validar rangos de presión
                    range_validation = self.validate_pressure_ranges(pressure_data)
                    
                    # Crear entrada de medición
                    measurement = {
                        'measurement_time': measurement_time.isoformat(),
                        'time_slot': time_slot,
                        'data': pressure_data,
                        'warnings': range_validation['warnings'],
                        'file_source': os.path.basename(csv_file)
                    }
                    
                    measurements.append(measurement)
                    logger.debug(f"Medición extraída: {measurement_time} - {time_slot} - {pressure_data}")
                    
                except Exception as e:
                    logger.warning(f"Error procesando fila {index}: {e}")
                    continue
            
            # Agrupar por franja horaria y mostrar resumen
            matutinas = [m for m in measurements if m['time_slot'] == 'matutina']
            vespertinas = [m for m in measurements if m['time_slot'] == 'vespertina']
            
            logger.info(f"📈 RESUMEN de {os.path.basename(csv_file)}:")
            logger.info(f"   🌅 Mediciones matutinas: {len(matutinas)}")
            logger.info(f"   🌆 Mediciones vespertinas: {len(vespertinas)}")
            logger.info(f"   📊 Total mediciones válidas: {len(measurements)}")
            
            return measurements
            
        except Exception as e:
            logger.error(f"Error extrayendo mediciones de {csv_file}: {e}")
            return measurements
    
    def extract_measurement_time(self, row: pd.Series, columns: Dict) -> Optional[datetime]:
        """
        Extrae la fecha/hora de una fila específica
        """
        # Buscar columna de fecha/hora
        date_column = None
        for col_type, col_name in columns.items():
            if col_type in ['date', 'time'] and col_name in row.index:
                date_column = col_name
                break
        
        if not date_column:
            return None
        
        try:
            date_value = row[date_column]
            if pd.isna(date_value):
                return None
            
            date_str = str(date_value)
            return self.parse_date_string(date_str)
            
        except Exception as e:
            logger.debug(f"Error extrayendo fecha de fila: {e}")
            return None
    
    def parse_date_string(self, date_str: str) -> Optional[datetime]:
        """
        Parsea una cadena de fecha/hora en varios formatos posibles
        """
        time_formats = [
            '%Y/%m/%d %H:%M',
            '%Y-%m-%d %H:%M:%S',
            '%d/%m/%Y %H:%M',
            '%m/%d/%Y %H:%M',
            '%Y/%m/%d %H:%M:%S',
            '%d-%m-%Y %H:%M',
            '%Y%m%d %H%M%S',
            '%H:%M:%S',
            '%H:%M'
        ]
        
        for fmt in time_formats:
            try:
                parsed_time = datetime.strptime(date_str, fmt)
                if fmt in ['%H:%M:%S', '%H:%M']:
                    today = datetime.now().date()
                    parsed_time = datetime.combine(today, parsed_time.time())
                
                return parsed_time
            except ValueError:
                continue
        
        # Intentar con pandas
        try:
            parsed_time = pd.to_datetime(date_str)
            if pd.notna(parsed_time):
                if hasattr(parsed_time, 'to_pydatetime'):
                    return parsed_time.to_pydatetime()
                return parsed_time
        except Exception:
            pass
        
        return None
    
    def detect_csv_columns(self, df: pd.DataFrame) -> Optional[Dict[str, str]]:
        """Detecta las columnas relevantes en el CSV"""
        columns = df.columns.str.lower()
        
        patterns = {
            'systolic': ['sistolic', 'systolic', 'sys', 'presion_sistolic', 'presión_sistólica', 'sys(mmhg)'],
            'diastolic': ['diastolic', 'diastolic', 'dia', 'presion_diastolic', 'presión_diastólica', 'dia(mmhg)'],
            'pulse': ['pulse', 'pulso', 'heart_rate', 'frecuencia', 'pulse(bpm)'],
            'date': ['date', 'fecha', 'timestamp', 'time', 'fecha de la medición', 'fecha de la medicion'],
            'time': ['time', 'hora', 'hour']
        }
        
        found_columns = {}
        
        for data_type, pattern_list in patterns.items():
            for col in columns:
                for pattern in pattern_list:
                    if pattern in col:
                        found_columns[data_type] = df.columns[columns.tolist().index(col)]
                        break
                if data_type in found_columns:
                    break
        
        logger.info(f"Columnas detectadas: {found_columns}")
        
        if 'systolic' in found_columns and 'diastolic' in found_columns:
            return found_columns
        
        return None
    
    def classify_time_slot(self, measurement_time: datetime) -> str:
        """
        Clasifica la hora de medición en una franja horaria
        Matutina: 04:00 - 12:59
        Vespertina: 13:00 - 03:59 (del día siguiente)
        """
        measurement_hour = measurement_time.hour
        
        if 4 <= measurement_hour <= 12:
            return 'matutina'
        elif 13 <= measurement_hour <= 23 or 0 <= measurement_hour <= 3:
            return 'vespertina'
        else:
            return 'fuera_de_horario'
    
    def validate_pressure_ranges(self, pressure_data: Dict) -> Dict:
        """Valida que los valores de presión estén en rangos normales"""
        result = {'warnings': []}
        
        for measurement, value in pressure_data.items():
            if measurement in self.pressure_ranges:
                min_val, max_val = self.pressure_ranges[measurement]
                if not (min_val <= value <= max_val):
                    result['warnings'].append(
                        f"{measurement.title()}: {value} fuera del rango normal ({min_val}-{max_val})"
                    )
        
        return result
    
    def process_patient_pressure_data(self, patient_dir: str) -> Dict:
        """
        Procesa todos los datos de presión de un paciente usando solo el mejor archivo CSV
        
        Args:
            patient_dir: Nombre del directorio del paciente
        
        Returns:
            Diccionario con todas las mediciones organizadas por día y franja
        """
        logger.info(f"🏥 Procesando datos de presión para paciente: {patient_dir}")
        
        # Encontrar el mejor archivo CSV
        best_csv = self.find_best_csv_file(patient_dir)
        if not best_csv:
            logger.warning(f"No se encontró archivo CSV válido para {patient_dir}")
            return {}
        
        # Extraer todas las mediciones del archivo
        measurements = self.extract_all_pressure_measurements(best_csv)
        if not measurements:
            logger.warning(f"No se pudieron extraer mediciones de {best_csv}")
            return {}
        
        # Organizar mediciones por día y franja horaria
        organized_data = {}
        
        for measurement in measurements:
            try:
                measurement_time = datetime.fromisoformat(measurement['measurement_time'])
                date_key = measurement_time.date().isoformat()
                time_slot = measurement['time_slot']
                
                # Inicializar estructura si no existe
                if date_key not in organized_data:
                    organized_data[date_key] = {
                        'matutina': [],
                        'vespertina': []
                    }
                
                # Agregar medición a la franja correspondiente
                if time_slot in organized_data[date_key]:
                    organized_data[date_key][time_slot].append(measurement)
                
            except Exception as e:
                logger.warning(f"Error organizando medición: {e}")
                continue
        
        # Mostrar resumen por día
        logger.info(f"📅 RESUMEN POR DÍA para {patient_dir}:")
        for date_key, day_data in sorted(organized_data.items()):
            matutinas = len(day_data['matutina'])
            vespertinas = len(day_data['vespertina'])
            
            status_matutina = "✅" if matutinas >= 2 else "❌"
            status_vespertina = "✅" if vespertinas >= 2 else "❌"
            
            logger.info(f"   📅 {date_key}:")
            logger.info(f"      🌅 Matutinas: {matutinas}/2 {status_matutina}")
            logger.info(f"      🌆 Vespertinas: {vespertinas}/2 {status_vespertina}")
        
        return organized_data

def test_csv_processor():
    """Prueba el procesador de CSV con algunos pacientes"""
    processor = ImprovedCSVProcessor()
    
    # Obtener lista de pacientes
    if not os.path.exists(processor.data_dir):
        print("❌ Directorio 'data' no encontrado")
        return
    
    patient_dirs = [d for d in os.listdir(processor.data_dir) 
                   if os.path.isdir(os.path.join(processor.data_dir, d))]
    
    print(f"\n🏥 Probando procesador CSV con {len(patient_dirs)} pacientes...")
    
    for patient_dir in patient_dirs[:3]:  # Probar con los primeros 3 pacientes
        print(f"\n{'='*60}")
        print(f"🏥 PACIENTE: {patient_dir}")
        print(f"{'='*60}")
        
        # Procesar datos de presión del paciente
        pressure_data = processor.process_patient_pressure_data(patient_dir)
        
        if pressure_data:
            print(f"✅ Datos procesados exitosamente")
            
            # Calcular estadísticas
            total_days = len(pressure_data)
            complete_days = 0
            
            for date_key, day_data in pressure_data.items():
                matutinas = len(day_data['matutina'])
                vespertinas = len(day_data['vespertina'])
                
                if matutinas >= 2 and vespertinas >= 2:
                    complete_days += 1
            
            print(f"📊 ESTADÍSTICAS:")
            print(f"   📅 Días con datos: {total_days}")
            print(f"   ✅ Días completos (2M+2V): {complete_days}")
            print(f"   📈 Completitud: {(complete_days/total_days*100):.1f}%" if total_days > 0 else "   📈 Completitud: 0%")
        else:
            print(f"❌ No se pudieron procesar datos")

if __name__ == "__main__":
    test_csv_processor()
