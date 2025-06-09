import pandas as pd
import PyPDF2
import pdfplumber
import os
import re
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional, Tuple
import logging
import signal
from contextlib import contextmanager
from content_based_ampm_resolver import ContentBasedAMPMResolver

logger = logging.getLogger(__name__)
ampm_logger = logging.getLogger('ampm_resolution')

@contextmanager
def timeout(duration):
    """Context manager para timeout de operaciones"""
    def timeout_handler(signum, frame):
        raise TimeoutError(f"Operación excedió {duration} segundos")
    
    # Configurar el timeout solo en sistemas Unix
    if hasattr(signal, 'SIGALRM'):
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(duration)
        try:
            yield
        finally:
            signal.alarm(0)
    else:
        # En Windows, simplemente ejecutar sin timeout
        yield

class FileValidator:
    def __init__(self):
        """Inicializa el validador de archivos mejorado"""
        # Definir franjas horarias
        self.time_slots = {
            'matutina': (time(4, 0), time(12, 59)),
            'vespertina': (time(13, 0), time(3, 59))  # 13:00 a 03:59 del día siguiente
        }
        
        # Rangos normales para validación
        self.pressure_ranges = {
            'systolic': (70, 250),
            'diastolic': (40, 150),
            'pulse': (40, 150)
        }
        
        # NUEVO: Inicializar el resolvedor de AM/PM basado en contenido
        self.ampm_resolver = ContentBasedAMPMResolver()
    
    def validate_csv_file(self, file_path: str) -> Dict:
        """
        Valida un archivo CSV de presión arterial y extrae todas las mediciones
        """
        validation_result = {
            'file_path': file_path,
            'is_valid': False,
            'errors': [],
            'warnings': [],
            'data': None,
            'measurement_time': None,
            'time_slot': None,
            'record_count': 0,
            'all_measurements': []  # Lista para almacenar todas las mediciones
        }
        
        try:
            if not os.path.exists(file_path):
                validation_result['errors'].append("Archivo no encontrado")
                return validation_result
            
            if os.path.getsize(file_path) == 0:
                validation_result['errors'].append("Archivo vacío")
                return validation_result
            
            # Leer CSV
            try:
                encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
                df = None
                
                for encoding in encodings:
                    try:
                        df = pd.read_csv(file_path, encoding=encoding)
                        logger.info(f"CSV leído exitosamente con encoding {encoding}")
                        break
                    except UnicodeDecodeError:
                        continue
                
                if df is None:
                    validation_result['errors'].append("No se pudo leer el archivo con ninguna codificación")
                    return validation_result
                
                validation_result['record_count'] = len(df)
                logger.info(f"CSV cargado: {len(df)} filas, columnas: {list(df.columns)}")
                
            except Exception as e:
                validation_result['errors'].append(f"Error leyendo CSV: {str(e)}")
                return validation_result
            
            # Validar estructura del CSV
            required_columns = self.detect_csv_columns(df)
            if not required_columns:
                validation_result['errors'].append("No se encontraron columnas de presión arterial válidas")
                return validation_result
            
            # Extraer todas las mediciones del CSV
            all_measurements = self.extract_all_measurements(df, required_columns, file_path)
            validation_result['all_measurements'] = all_measurements
            
            # Si hay al menos una medición, considerar el archivo válido
            if all_measurements:
                validation_result['is_valid'] = True
                
                # Usar la primera medición para los datos principales
                first_measurement = all_measurements[0]
                validation_result['data'] = first_measurement['data']
                validation_result['measurement_time'] = first_measurement['measurement_time']
                validation_result['time_slot'] = first_measurement['time_slot']
                
                logger.info(f"Extraídas {len(all_measurements)} mediciones del archivo CSV")
            else:
                validation_result['errors'].append("No se pudieron extraer mediciones válidas")
            
        except Exception as e:
            validation_result['errors'].append(f"Error inesperado validando CSV: {str(e)}")
        
        return validation_result
    
    def extract_all_measurements(self, df: pd.DataFrame, columns: Dict[str, str], file_path: str) -> List[Dict]:
        """
        Extrae todas las mediciones del DataFrame
        """
        measurements = []
        
        # Identificar columna de fecha/hora
        date_column = None
        for col_type, col_name in columns.items():
            if col_type in ['date', 'time'] and col_name in df.columns:
                date_column = col_name
                break
        
        if not date_column:
            logger.warning(f"No se encontró columna de fecha/hora en {file_path}")
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
                date_value = row[date_column]
                if pd.isna(date_value):
                    continue
                
                date_str = str(date_value)
                measurement_time = self.parse_date_string(date_str)
                
                if not measurement_time:
                    continue
                
                # Clasificar en franja horaria
                time_slot = self.classify_time_slot(measurement_time)
                
                # Validar rangos de presión
                range_validation = self.validate_pressure_ranges(pressure_data)
                
                # Crear entrada de medición
                measurement = {
                    'data': pressure_data,
                    'measurement_time': measurement_time.isoformat(),
                    'time_slot': time_slot,
                    'warnings': range_validation['warnings']
                }
                
                measurements.append(measurement)
                logger.debug(f"Medición extraída: {measurement_time} - {time_slot} - {pressure_data}")
                
            except Exception as e:
                logger.warning(f"Error procesando fila {index}: {e}")
                continue
        
        return measurements
    
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
    
    def extract_measurement_time_from_csv(self, df: pd.DataFrame, file_path: str) -> Optional[datetime]:
        """Extrae la hora de medición del contenido del CSV"""
        
        # Buscar en las columnas de fecha/hora del CSV
        for col in df.columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in ['fecha', 'date', 'time', 'hora', 'timestamp', 'medición', 'medicion']):
                try:
                    time_values = df[col].dropna()
                    if len(time_values) > 0:
                        time_str = str(time_values.iloc[0])
                        logger.info(f"Intentando parsear fecha de columna '{col}': '{time_str}'")
                        
                        parsed_time = self.parse_date_string(time_str)
                        if parsed_time:
                            logger.info(f"Fecha parseada exitosamente: {parsed_time}")
                            return parsed_time
                        
                except Exception as e:
                    logger.warning(f"Error procesando columna {col}: {e}")
                    continue
        
        # Si no se encuentra en el contenido, intentar extraer del nombre del archivo
        filename = os.path.basename(file_path)
        logger.info(f"Intentando extraer fecha del nombre del archivo: {filename}")
        
        patterns = [
            r'pressure_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})',
            r'(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})',
            r'(\d{4}-\d{2}-\d{2})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                try:
                    groups = match.groups()
                    if len(groups) == 2:
                        date_str, time_str = groups
                        datetime_str = f"{date_str} {time_str.replace('-', ':')}"
                        result = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
                        logger.info(f"Fecha y hora extraídas del nombre: {result}")
                        return result
                    elif len(groups) == 6:
                        year, month, day, hour, minute, second = map(int, groups)
                        result = datetime(year, month, day, hour, minute, second)
                        logger.info(f"Fecha completa extraída del nombre: {result}")
                        return result
                    elif len(groups) == 1:
                        result = datetime.strptime(groups[0], '%Y-%m-%d')
                        result = result.replace(hour=12)
                        logger.info(f"Fecha extraída del nombre (YYYY-MM-DD): {result}")
                        return result
                except ValueError as e:
                    logger.warning(f"Error parseando fecha del nombre con patrón {pattern}: {e}")
                    continue
        
        logger.warning(f"No se pudo extraer fecha de {file_path}, usando fecha actual")
        return datetime.now()
    
    def validate_pdf_file(self, file_path: str) -> Dict:
        """Valida un archivo PDF de ECG con timeout"""
        validation_result = {
            'file_path': file_path,
            'is_valid': False,
            'errors': [],
            'warnings': [],
            'patient_name': None,
            'measurement_time': None,
            'time_slot': None,
            'content_summary': None,
            'has_am_pm_ambiguity': False
        }
        
        try:
            if not os.path.exists(file_path):
                validation_result['errors'].append("Archivo no encontrado")
                return validation_result
            
            if os.path.getsize(file_path) == 0:
                validation_result['errors'].append("Archivo vacío")
                return validation_result
        
            # Verificar tamaño del archivo (límite de 10MB)
            file_size = os.path.getsize(file_path)
            if file_size > 10 * 1024 * 1024:  # 10MB
                validation_result['errors'].append(f"Archivo demasiado grande: {file_size / 1024 / 1024:.1f}MB")
                return validation_result
        
            logger.info(f"Procesando PDF: {file_path} ({file_size / 1024:.1f}KB)")
        
            try:
                # NUEVO: Usar timeout para evitar colgarse
                with timeout(30):  # 30 segundos máximo por PDF
                    with pdfplumber.open(file_path) as pdf:
                        if len(pdf.pages) == 0:
                            validation_result['errors'].append("PDF sin páginas")
                            return validation_result
                    
                        # Limitar a las primeras 3 páginas para evitar PDFs enormes
                        max_pages = min(3, len(pdf.pages))
                        full_text = ""
                    
                        for i in range(max_pages):
                            try:
                                page_text = pdf.pages[i].extract_text()
                                if page_text:
                                    full_text += page_text + "\n"
                                    # Si ya tenemos suficiente texto, parar
                                    if len(full_text) > 5000:  # Límite de 5000 caracteres
                                        break
                            except Exception as e:
                                logger.warning(f"Error extrayendo texto de página {i}: {e}")
                                continue
                    
                        if not full_text.strip():
                            validation_result['errors'].append("PDF sin contenido de texto legible")
                            return validation_result
                    
                        logger.info(f"Texto extraído del PDF ({len(full_text)} caracteres): {full_text[:200]}...")
                    
                        content_analysis = self.analyze_ecg_content(full_text, file_path)
                        validation_result.update(content_analysis)
                    
            except TimeoutError:
                validation_result['errors'].append("Timeout procesando PDF (>30 segundos)")
                logger.error(f"Timeout procesando PDF: {file_path}")
                return validation_result
            except Exception as e:
                validation_result['errors'].append(f"Error leyendo PDF: {str(e)}")
                logger.error(f"Error leyendo PDF {file_path}: {e}")
                return validation_result
        
            validation_result['is_valid'] = len(validation_result['errors']) == 0
        
        except Exception as e:
            validation_result['errors'].append(f"Error inesperado validando PDF: {str(e)}")
            logger.error(f"Error inesperado validando PDF {file_path}: {e}")
    
        return validation_result
    
    def analyze_ecg_content(self, text: str, file_path: str) -> Dict:
        """Analiza el contenido del PDF de ECG con detección precisa de ambigüedad AM/PM"""
        analysis = {
            'patient_name': None,
            'measurement_time': None,
            'time_slot': None,
            'content_summary': {},
            'warnings': [],
            'has_am_pm_ambiguity': False,
            'original_time': None
        }
        
        ampm_logger.info(f"=== ANALIZANDO ECG: {file_path} ===")
        ampm_logger.info(f"Texto completo del PDF:\n{text}")
        
        try:
            # Buscar nombre del paciente
            name_patterns = [
                r'[Pp]aciente:?\s*([A-Za-zÁÉÍÓÚáéíóúñÑ\-\s]+)',
                r'[Pp]aciente([A-Za-zÁÉÍÓÚáéíóúñÑ\-\s]+)',
                r'[Nn]ombre:?\s*([A-Za-zÁÉÍÓÚáéíóúñÑ\-\s]+)',
                r'[Pp]atient:?\s*([A-Za-zÁÉÍÓÚáéíóúñÑ\-\s]+)',
                r'Registrado\s*([A-Za-zÁÉÍÓÚáéíóúñÑ\-\s]+)'
            ]
            
            for pattern in name_patterns:
                try:
                    match = re.search(pattern, text)
                    if match:
                        name = match.group(1).strip()
                        if len(name) > 2:
                            clean_name = re.sub(r'[^\w\s\-]', '', name)
                            analysis['patient_name'] = clean_name.title()
                            break
                except Exception as e:
                    logger.warning(f"Error procesando patrón de nombre: {e}")
                    continue
            
            # Extraer nombre del paciente del directorio si no se encontró
            if not analysis['patient_name']:
                try:
                    parent_dir = os.path.basename(os.path.dirname(file_path))
                    if parent_dir and parent_dir != "data":
                        patient_name = parent_dir.split('_')[0] if '_' in parent_dir else parent_dir
                        if patient_name:
                            analysis['patient_name'] = patient_name
                except Exception as e:
                    logger.warning(f"Error extrayendo nombre del paciente del directorio: {e}")
            
            # DETECCIÓN PRECISA DE INDICADORES AM/PM
            # Buscar específicamente patrones de AM/PM cerca de las horas
            ampm_patterns = [
                r'\d{1,2}:\d{2}:\d{2}\s*[ap]\.?m\.?',  # 8:15:26 a.m. o 8:15:26 pm
                r'\d{1,2}:\d{2}\s*[ap]\.?m\.?',        # 8:15 a.m. o 8:15 pm
                r'[ap]\.?m\.?\s*\d{1,2}:\d{2}',        # a.m. 8:15 o pm 8:15
            ]
            
            has_explicit_ampm = False
            for pattern in ampm_patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    has_explicit_ampm = True
                    ampm_logger.info(f"Indicador AM/PM explícito encontrado: {pattern}")
                    break
            
            ampm_logger.info(f"¿Tiene indicadores AM/PM explícitos? {has_explicit_ampm}")
            
            # BUSCAR FECHA Y HORA
            datetime_patterns = [
                # Patrón principal con AM/PM: "Registrado jueves, 13 de mar de 2025, 2:05:59 p. m."
                r'[Rr]egistrado\w*\s*\w+,\s*(\d{1,2})\s*de\s*(\w+)\s*de\s*(\d{4}),\s*(\d{1,2}):(\d{2}):(\d{2})\s*([ap]\.?\s*m\.?)',
                # Patrón principal sin AM/PM: "Registrado jueves, 22 de may de 2025, 8:15:26"
                r'[Rr]egistrado\w*\s*\w+,\s*(\d{1,2})\s*de\s*(\w+)\s*de\s*(\d{4}),\s*(\d{1,2}):(\d{2}):(\d{2})',
                # Patrón alternativo: "22 de mayo de 2025, 8:15:26"
                r'(\d{1,2})\s*de\s*(\w+)\s*de\s*(\d{4}),\s*(\d{1,2}):(\d{2}):(\d{2})',
                # Patrón con "Fecha de registro": "viernes, 4 de abril de 2025, 6:14:36 p.m."
                r'[Ff]echa\s+de\s+registro:?\s*\w+,\s*(\d{1,2})\s*de\s*(\w+)\s*de\s*(\d{4}),\s*(\d{1,2}):(\d{2}):(\d{2})\s*([ap]\.?m\.?)?',
            ]
            
            months_es = {
                'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'may': 5,
                'junio': 6, 'julio': 7, 'agosto': 8, 'septiembre': 9, 'octubre': 10,
                'noviembre': 11, 'diciembre': 12
            }
            
            for pattern_idx, pattern in enumerate(datetime_patterns):
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    groups = match.groups()
                    ampm_logger.info(f"Patrón {pattern_idx} encontrado: {groups}")
                    
                    try:
                        # Manejar diferentes formatos de grupos según el patrón
                        if pattern_idx == 0:  # Patrón con AM/PM capturado
                            day, month_name, year, hour, minute, second, explicit_ampm = groups
                        elif len(groups) >= 6:
                            day, month_name, year, hour, minute, second = groups[:6]
                            # Verificar si hay AM/PM en el grupo 7 (para el patrón de "Fecha de registro")
                            explicit_ampm = groups[6] if len(groups) > 6 and groups[6] else None
                        else:
                            continue
                        
                        month = months_es.get(month_name.lower())
                        
                        if month:
                            hour_int = int(hour)
                            
                            # Crear datetime inicial
                            measurement_time = datetime(
                                int(year), month, int(day),
                                hour_int, int(minute), int(second)
                            )
                            
                            ampm_logger.info(f"Tiempo inicial extraído: {measurement_time}")
                            ampm_logger.info(f"AM/PM explícito en match: {explicit_ampm}")
                            
                            # LÓGICA DE AMBIGÜEDAD CORREGIDA
                            # Es ambiguo si:
                            # 1. La hora está entre 1-12 Y
                            # 2. NO hay indicadores AM/PM explícitos en el texto Y
                            # 3. NO hay AM/PM en el match específico
                            is_ambiguous_hour = 1 <= hour_int <= 12
                            has_ampm_in_match = explicit_ampm is not None
                            
                            is_ambiguous = (is_ambiguous_hour and 
                                          not has_explicit_ampm and 
                                          not has_ampm_in_match)
                            
                            ampm_logger.info(f"Análisis de ambigüedad:")
                            ampm_logger.info(f"  - Hora ambigua (1-12): {is_ambiguous_hour}")
                            ampm_logger.info(f"  - AM/PM explícito en texto: {has_explicit_ampm}")
                            ampm_logger.info(f"  - AM/PM en match: {has_ampm_in_match}")
                            ampm_logger.info(f"  - ES AMBIGUO: {is_ambiguous}")
                            
                            if is_ambiguous:
                                analysis['has_am_pm_ambiguity'] = True
                                analysis['original_time'] = measurement_time.isoformat()
                                ampm_logger.warning(f"🚨 AMBIGÜEDAD DETECTADA: {hour_int}:{minute}:{second}")
                                
                                # NUEVO: Usar el resolvedor basado en contenido
                                patient_dir = os.path.basename(os.path.dirname(file_path))
                                resolved_time = self.ampm_resolver.resolve_ecg_ambiguity(measurement_time, patient_dir)
                                
                                analysis['measurement_time'] = resolved_time.isoformat()
                                analysis['time_slot'] = self.classify_time_slot(resolved_time)
                                
                                ampm_logger.info(f"✅ RESOLUCIÓN: {measurement_time} -> {resolved_time}")
                                ampm_logger.info(f"✅ FRANJA: {analysis['time_slot']}")
                            else:
                                # No hay ambigüedad
                                # Si hay AM/PM explícito, ajustar la hora
                                if explicit_ampm:
                                    ampm_clean = explicit_ampm.lower().replace('.', '').replace(' ', '')
                                    if ampm_clean.startswith('p') and hour_int < 12:
                                        measurement_time = measurement_time.replace(hour=hour_int + 12)
                                        ampm_logger.info(f"Ajustado a PM: {measurement_time}")
                                    elif ampm_clean.startswith('a') and hour_int == 12:
                                        measurement_time = measurement_time.replace(hour=0)
                                        ampm_logger.info(f"Ajustado a AM (medianoche): {measurement_time}")
                                
                                analysis['measurement_time'] = measurement_time.isoformat()
                                analysis['time_slot'] = self.classify_time_slot(measurement_time)
                                ampm_logger.info(f"✅ SIN AMBIGÜEDAD: {measurement_time} -> {analysis['time_slot']}")
                
                            break
                    except Exception as e:
                        ampm_logger.error(f"Error procesando grupos: {e}")
                        continue
            
            # Si no se encuentra fecha en el texto, usar nombre del archivo
            if not analysis['measurement_time']:
                ampm_logger.warning("No se encontró fecha en el texto, usando nombre del archivo")
                filename = os.path.basename(file_path)
                
                # Intentar extraer del nombre del archivo
                patterns = [
                    r'ecg_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})',
                    r'(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})',
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, filename)
                    if match:
                        groups = match.groups()
                        if len(groups) == 2:
                            date_str, time_str = groups
                            datetime_str = f"{date_str} {time_str.replace('-', ':')}"
                            measurement_time = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
                        elif len(groups) == 6:
                            year, month, day, hour, minute, second = map(int, groups)
                            measurement_time = datetime(year, month, day, hour, minute, second)
                        
                        analysis['measurement_time'] = measurement_time.isoformat()
                        analysis['time_slot'] = self.classify_time_slot(measurement_time)
                        ampm_logger.info(f"Fecha del archivo: {measurement_time} -> {analysis['time_slot']}")
                        break
            
            # Analizar contenido del ECG
            ecg_keywords = ['ecg', 'electrocardiogram', 'ritmo', 'frecuencia', 'bpm', 'latido', 'cardíaca']
            found_keywords = [kw for kw in ecg_keywords if kw.lower() in text.lower()]
            
            analysis['content_summary'] = {
                'text_length': len(text),
                'ecg_keywords_found': found_keywords,
                'has_ecg_content': len(found_keywords) > 0
            }
            
            if not analysis['content_summary']['has_ecg_content']:
                analysis['warnings'].append("No se detectó contenido típico de ECG en el PDF")
        
        except Exception as e:
            ampm_logger.error(f"Error general analizando ECG: {e}")
            analysis['warnings'].append(f"Error analizando contenido: {str(e)}")
        
        ampm_logger.info(f"=== RESULTADO FINAL: {analysis['time_slot']} ===")
        return analysis
    
    def resolve_am_pm_with_pressure(self, ecg_date: datetime, patient_dir: str) -> datetime:
        """
        NUEVO MÉTODO: Resuelve ambigüedad AM/PM usando el contenido de archivos de presión
        """
        try:
            # Usar el nuevo resolvedor basado en contenido
            patient_name = os.path.basename(patient_dir)
            resolved_date = self.ampm_resolver.resolve_ecg_ambiguity(ecg_date, patient_name)
            
            if resolved_date != ecg_date:
                ampm_logger.info(f"✅ Ambigüedad resuelta con contenido: {ecg_date} -> {resolved_date}")
            else:
                ampm_logger.info(f"ℹ️ No se cambió la fecha: {ecg_date}")
            
            return resolved_date
            
        except Exception as e:
            ampm_logger.error(f"❌ Error en resolución AM/PM: {e}")
            return ecg_date
