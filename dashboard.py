import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import os
from datetime import datetime, timedelta
from monitoring_system import MonitoringSystem
import logging

# Configurar página
st.set_page_config(
    page_title="Sistema de Monitoreo Médico",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configurar logging para Streamlit
logging.basicConfig(level=logging.INFO)

class MedicalDashboard:
    def __init__(self):
        """Inicializa el dashboard médico"""
        self.monitoring_system = MonitoringSystem()
        
    def load_latest_report(self):
        """Carga el reporte más reciente"""
        reports_path = self.monitoring_system.reports_path
        
        if not os.path.exists(reports_path):
            return None
        
        # Buscar el archivo de reporte más reciente
        report_files = [f for f in os.listdir(reports_path) if f.startswith('monitoring_report_') and f.endswith('.json')]
        
        if not report_files:
            return None
        
        # Ordenar por fecha de modificación
        report_files.sort(key=lambda x: os.path.getmtime(os.path.join(reports_path, x)), reverse=True)
        latest_file = report_files[0]
        
        try:
            with open(os.path.join(reports_path, latest_file), 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            st.error(f"Error cargando reporte: {e}")
            return None
    
    def run_dashboard(self):
        """Ejecuta el dashboard principal"""
        st.title("🏥 Sistema de Monitoreo Médico")
        st.markdown("---")
        
        # Sidebar para controles
        with st.sidebar:
            st.header("⚙️ Controles")
            
            # Botón para ejecutar chequeo manual
            if st.button("🔄 Ejecutar Chequeo Manual", type="primary"):
                with st.spinner("Ejecutando chequeo..."):
                    try:
                        summary = self.monitoring_system.run_daily_check()
                        st.success("Chequeo completado exitosamente")
                        
                        # Mostrar resumen rápido
                        st.metric("Emails Procesados", summary['emails_processed'])
                        st.metric("Archivos Validados", summary['files_validated'])
                        
                        if summary['errors']:
                            st.error(f"Errores encontrados: {len(summary['errors'])}")
                        
                        if summary['warnings']:
                            st.warning(f"Advertencias: {len(summary['warnings'])}")
                        
                        # Recargar la página para mostrar nuevos datos
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Error ejecutando chequeo: {e}")
            
            st.markdown("---")
            
            # Configuración de filtros
            st.subheader("📊 Filtros")
            
            # Selector de período
            period_options = {
                "Últimos 7 días": 7,
                "Últimos 14 días": 14,
                "Último mes": 30
            }
            
            selected_period = st.selectbox(
                "Período de análisis",
                options=list(period_options.keys()),
                index=0
            )
            
            days_back = period_options[selected_period]
        
        # Cargar datos del reporte más reciente
        report_data = self.load_latest_report()
        
        if not report_data:
            st.warning("No hay datos de reporte disponibles. Ejecute un chequeo manual para generar datos.")
            return
        
        # Mostrar información del reporte
        report_date = datetime.fromisoformat(report_data['generation_date'].replace('Z', '+00:00'))
        st.info(f"📅 Último reporte generado: {report_date.strftime('%d/%m/%Y %H:%M:%S')}")
        
        # Métricas principales
        self.show_main_metrics(report_data)
        
        # Gráficos y análisis
        col1, col2 = st.columns(2)
        
        with col1:
            self.show_completion_chart(report_data)
            self.show_patient_status_table(report_data)
        
        with col2:
            self.show_timeline_chart(report_data)
            self.show_missing_measurements(report_data)
        
        # Tabla detallada de pacientes
        st.markdown("---")
        self.show_detailed_patient_table(report_data)
        
        # Alertas y recomendaciones
        st.markdown("---")
        self.show_alerts_and_recommendations(report_data)
    
    def show_main_metrics(self, report_data):
        """Muestra las métricas principales en la parte superior"""
        overall = report_data['overall_summary']
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "👥 Total Pacientes",
                overall['total_patients']
            )
        
        with col2:
            completion_rate = (overall['patients_complete'] / overall['total_patients'] * 100) if overall['total_patients'] > 0 else 0
            st.metric(
                "✅ Pacientes Completos",
                overall['patients_complete'],
                f"{completion_rate:.1f}%"
            )
        
        with col3:
            measurement_rate = (overall['total_measurements_received'] / overall['total_measurements_expected'] * 100) if overall['total_measurements_expected'] > 0 else 0
            st.metric(
                "📊 Mediciones Recibidas",
                f"{overall['total_measurements_received']}/{overall['total_measurements_expected']}",
                f"{measurement_rate:.1f}%"
            )
        
        with col4:
            st.metric(
                "⚠️ Pacientes Incompletos",
                overall['patients_incomplete']
            )
    
    def show_completion_chart(self, report_data):
        """Muestra gráfico de completitud por paciente"""
        st.subheader("📈 Completitud por Paciente")
        
        # Preparar datos
        patients_data = []
        for patient_name, patient_info in report_data['patients'].items():
            patients_data.append({
                'Paciente': patient_name,
                'Completitud (%)': patient_info['completion_percentage'],
                'Estado': 'Completo' if patient_info['is_complete'] else 'Incompleto'
            })
        
        if not patients_data:
            st.warning("No hay datos de pacientes disponibles")
            return
        
        df = pd.DataFrame(patients_data)
        
        # Crear gráfico de barras
        fig = px.bar(
            df,
            x='Paciente',
            y='Completitud (%)',
            color='Estado',
            color_discrete_map={'Completo': '#28a745', 'Incompleto': '#dc3545'},
            title="Porcentaje de Completitud por Paciente"
        )
        
        fig.update_layout(
            xaxis_tickangle=-45,
            height=400,
            showlegend=True
        )
        
        # Agregar línea de referencia en 100%
        fig.add_hline(y=100, line_dash="dash", line_color="gray", annotation_text="Meta 100%")
        
        st.plotly_chart(fig, use_container_width=True)
    
    def show_timeline_chart(self, report_data):
        """Muestra gráfico de timeline de mediciones"""
        st.subheader("📅 Timeline de Mediciones")
        
        # Preparar datos para el timeline
        timeline_data = []
        
        for patient_name, patient_info in report_data['patients'].items():
            daily_data = patient_info.get('daily_data', {})
            
            for date_str, day_data in daily_data.items():
                for time_slot, measurements in day_data.items():
                    if measurements['pressure'] or measurements['ecg']:
                        timeline_data.append({
                            'Paciente': patient_name,
                            'Fecha': date_str,
                            'Franja': time_slot,
                            'Presión': measurements['pressure'],
                            'ECG': measurements['ecg'],
                            'Completo': measurements['pressure'] and measurements['ecg']
                        })
        
        if not timeline_data:
            st.warning("No hay datos de timeline disponibles")
            return
        
        df_timeline = pd.DataFrame(timeline_data)
        df_timeline['Fecha'] = pd.to_datetime(df_timeline['Fecha'])
        
        # Crear gráfico de dispersión
        fig = px.scatter(
            df_timeline,
            x='Fecha',
            y='Paciente',
            color='Completo',
            symbol='Franja',
            color_discrete_map={True: '#28a745', False: '#ffc107'},
            title="Timeline de Mediciones por Paciente"
        )
        
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    def show_patient_status_table(self, report_data):
        """Muestra tabla de estado de pacientes"""
        st.subheader("👥 Estado de Pacientes")
        
        # Preparar datos para la tabla
        table_data = []
        for patient_name, patient_info in report_data['patients'].items():
            table_data.append({
                'Paciente': patient_name,
                'Completitud': f"{patient_info['completion_percentage']:.1f}%",
                'Recibidas': patient_info['received_measurements'],
                'Esperadas': patient_info['expected_measurements'],
                'Estado': '✅ Completo' if patient_info['is_complete'] else '⚠️ Incompleto'
            })
        
        if table_data:
            df_table = pd.DataFrame(table_data)
            st.dataframe(df_table, use_container_width=True, hide_index=True)
        else:
            st.warning("No hay datos de pacientes disponibles")
    
    def show_missing_measurements(self, report_data):
        """Muestra mediciones faltantes"""
        st.subheader("❌ Mediciones Faltantes")
        
        missing_data = []
        for patient_name, patient_info in report_data['patients'].items():
            for missing in patient_info.get('missing_measurements', []):
                missing_data.append({
                    'Paciente': patient_name,
                    'Fecha': missing['date'],
                    'Franja': missing['time_slot'],
                    'Faltantes': ', '.join(missing['missing'])
                })
        
        if missing_data:
            df_missing = pd.DataFrame(missing_data)
            
            # Mostrar solo las más recientes (últimos 3 días)
            df_missing['Fecha'] = pd.to_datetime(df_missing['Fecha'])
            recent_date = df_missing['Fecha'].max() - timedelta(days=3)
            df_recent = df_missing[df_missing['Fecha'] >= recent_date]
            
            if not df_recent.empty:
                st.dataframe(df_recent.sort_values('Fecha', ascending=False), use_container_width=True, hide_index=True)
            else:
                st.success("No hay mediciones faltantes recientes")
        else:
            st.success("No hay mediciones faltantes")
    
    def show_detailed_patient_table(self, report_data):
        """Muestra tabla detallada expandible por paciente"""
        st.subheader("📋 Detalle por Paciente")
        
        for patient_name, patient_info in report_data['patients'].items():
            with st.expander(f"👤 {patient_name} - {patient_info['completion_percentage']:.1f}% completo"):
                
                # Métricas del paciente
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Mediciones Recibidas", patient_info['received_measurements'])
                
                with col2:
                    st.metric("Mediciones Esperadas", patient_info['expected_measurements'])
                
                with col3:
                    status_color = "🟢" if patient_info['is_complete'] else "🟡"
                    st.metric("Estado", f"{status_color} {'Completo' if patient_info['is_complete'] else 'Incompleto'}")
                
                # Tabla de mediciones diarias
                daily_data = patient_info.get('daily_data', {})
                if daily_data:
                    daily_table = []
                    
                    for date_str, day_data in daily_data.items():
                        for time_slot, measurements in day_data.items():
                            daily_table.append({
                                'Fecha': date_str,
                                'Franja Horaria': time_slot,
                                'Presión': '✅' if measurements['pressure'] else '❌',
                                'ECG': '✅' if measurements['ecg'] else '❌',
                                'Completo': '✅' if measurements['pressure'] and measurements['ecg'] else '❌'
                            })
                    
                    if daily_table:
                        df_daily = pd.DataFrame(daily_table)
                        df_daily = df_daily.sort_values(['Fecha', 'Franja Horaria'])
                        st.dataframe(df_daily, use_container_width=True, hide_index=True)
                
                # Mediciones faltantes específicas
                missing_measurements = patient_info.get('missing_measurements', [])
                if missing_measurements:
                    st.write("**Mediciones Faltantes:**")
                    for missing in missing_measurements[-5:]:  # Mostrar últimas 5
                        st.write(f"- {missing['date']} ({missing['time_slot']}): {', '.join(missing['missing'])}")
    
    def show_alerts_and_recommendations(self, report_data):
        """Muestra alertas y recomendaciones"""
        st.subheader("🚨 Alertas y Recomendaciones")
        
        alerts = []
        recommendations = []
        
        # Analizar datos para generar alertas
        for patient_name, patient_info in report_data['patients'].items():
            completion = patient_info['completion_percentage']
            
            if completion < 50:
                alerts.append(f"🔴 **{patient_name}**: Completitud muy baja ({completion:.1f}%)")
            elif completion < 80:
                alerts.append(f"🟡 **{patient_name}**: Completitud baja ({completion:.1f}%)")
            
            # Verificar mediciones recientes
            missing_recent = [m for m in patient_info.get('missing_measurements', []) 
                            if datetime.fromisoformat(m['date']) >= datetime.now().date() - timedelta(days=2)]
            
            if missing_recent:
                alerts.append(f"⚠️ **{patient_name}**: Faltan mediciones de los últimos 2 días")
        
        # Generar recomendaciones
        overall = report_data['overall_summary']
        
        if overall['patients_incomplete'] > 0:
            recommendations.append("📞 Contactar pacientes con completitud baja para recordar las mediciones")
        
        if overall['total_measurements_received'] < overall['total_measurements_expected'] * 0.8:
            recommendations.append("📧 Enviar recordatorios automáticos por email")
        
        # Mostrar alertas
        if alerts:
            st.write("**Alertas Activas:**")
            for alert in alerts:
                st.markdown(alert)
        else:
            st.success("✅ No hay alertas activas")
        
        # Mostrar recomendaciones
        if recommendations:
            st.write("**Recomendaciones:**")
            for rec in recommendations:
                st.markdown(rec)

# Función principal para ejecutar el dashboard
def main():
    dashboard = MedicalDashboard()
    dashboard.run_dashboard()

if __name__ == "__main__":
    main()
