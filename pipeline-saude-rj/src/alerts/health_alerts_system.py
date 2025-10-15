"""
Sistema de Alertas Automáticos para Ocupação Crítica
Monitora métricas de saúde e dispara alertas via Slack/Email
"""

import pandas as pd
import json
import requests
import smtplib
try:
    from email.mime.text import MimeText
    from email.mime.multipart import MimeMultipart
except ImportError:
    # Fallback para versões mais antigas
    MimeText = None
    MimeMultipart = None
from datetime import datetime, timedelta
import os
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class HealthAlertsSystem:
    """Sistema de alertas para monitoramento de saúde pública"""
    
    def __init__(self):
        """Inicializa configurações de alertas"""
        
        # Configurações de alertas
        self.alert_config = {
            'occupancy_thresholds': {
                'uti_critical': 85,      # UTI > 85% = crítico
                'uti_warning': 70,       # UTI > 70% = atenção
                'general_critical': 90,   # Leitos > 90% = crítico
                'general_warning': 75     # Leitos > 75% = atenção
            },
            'wait_time_thresholds': {
                'emergency_critical': 30,    # Emergência > 30min = crítico
                'emergency_warning': 15,     # Emergência > 15min = atenção  
                'general_critical': 120,     # Geral > 2h = crítico
                'general_warning': 60        # Geral > 1h = atenção
            }
        }
        
        # Configurações de notificação
        self.slack_webhook = os.getenv('SLACK_WEBHOOK_URL')
        self.email_config = {
            'smtp_server': os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
            'smtp_port': int(os.getenv('SMTP_PORT', '587')),
            'sender_email': os.getenv('SENDER_EMAIL'),
            'sender_password': os.getenv('SENDER_PASSWORD'),
            'recipient_emails': os.getenv('RECIPIENT_EMAILS', '').split(',')
        }
        
    def analyze_occupancy_alerts(self, df_leitos: pd.DataFrame) -> List[Dict]:
        """Analisa dados de leitos e identifica alertas de ocupação"""
        
        alerts = []
        
        # UTI Crítica (> 85%)
        uti_critical = df_leitos[
            df_leitos['percentual_ocupacao_uti'] > self.alert_config['occupancy_thresholds']['uti_critical']
        ]
        
        if not uti_critical.empty:
            alerts.append({
                'type': 'UTI_CRITICAL',
                'severity': 'CRITICAL',
                'title': '🚨 OCUPAÇÃO UTI CRÍTICA',
                'message': f"{len(uti_critical)} hospitais com UTI >85% ocupada",
                'details': {
                    'affected_hospitals': uti_critical[['nome_unidade', 'percentual_ocupacao_uti']].to_dict('records'),
                    'avg_occupancy': round(uti_critical['percentual_ocupacao_uti'].mean(), 1),
                    'max_occupancy': uti_critical['percentual_ocupacao_uti'].max()
                },
                'actions_required': [
                    "Ativar protocolo de transferência de pacientes",
                    "Contatar hospitais com vagas disponíveis", 
                    "Alertar equipes médicas sobre capacidade crítica"
                ]
            })
        
        # UTI Atenção (70-85%)
        uti_warning = df_leitos[
            (df_leitos['percentual_ocupacao_uti'] > self.alert_config['occupancy_thresholds']['uti_warning']) &
            (df_leitos['percentual_ocupacao_uti'] <= self.alert_config['occupancy_thresholds']['uti_critical'])
        ]
        
        if not uti_warning.empty:
            alerts.append({
                'type': 'UTI_WARNING',
                'severity': 'WARNING',
                'title': '⚠️ OCUPAÇÃO UTI ATENÇÃO',
                'message': f"{len(uti_warning)} hospitais com UTI entre 70-85% ocupada",
                'details': {
                    'affected_hospitals': uti_warning[['nome_unidade', 'percentual_ocupacao_uti']].to_dict('records'),
                    'avg_occupancy': round(uti_warning['percentual_ocupacao_uti'].mean(), 1)
                },
                'actions_required': [
                    "Monitorar ocupação a cada 2 horas",
                    "Preparar planos de transferência preventiva"
                ]
            })
        
        # Leitos Gerais Críticos (> 90%)
        general_critical = df_leitos[
            df_leitos['percentual_ocupacao'] > self.alert_config['occupancy_thresholds']['general_critical']
        ]
        
        if not general_critical.empty:
            alerts.append({
                'type': 'GENERAL_CRITICAL',
                'severity': 'CRITICAL',
                'title': '🏥 OCUPAÇÃO GERAL CRÍTICA',
                'message': f"{len(general_critical)} hospitais com ocupação >90%",
                'details': {
                    'affected_hospitals': general_critical[['nome_unidade', 'percentual_ocupacao']].to_dict('records'),
                    'avg_occupancy': round(general_critical['percentual_ocupacao'].mean(), 1)
                },
                'actions_required': [
                    "Acelerar altas médicas quando possível",
                    "Ativar leitos de retaguarda",
                    "Coordenar com rede privada"
                ]
            })
        
        return alerts
    
    def analyze_wait_time_alerts(self, df_atendimentos: pd.DataFrame) -> List[Dict]:
        """Analisa tempos de espera e identifica alertas"""
        
        alerts = []
        
        # Emergências com espera > 30min
        emergency_critical = df_atendimentos[
            (df_atendimentos['tipo_atendimento'] == 'Emergencia') &
            (df_atendimentos['tempo_espera_minutos'] > self.alert_config['wait_time_thresholds']['emergency_critical'])
        ]
        
        if not emergency_critical.empty:
            alerts.append({
                'type': 'EMERGENCY_WAIT_CRITICAL',
                'severity': 'CRITICAL',
                'title': '🚑 TEMPO DE ESPERA EMERGÊNCIA CRÍTICO',
                'message': f"{len(emergency_critical)} emergências com espera >{self.alert_config['wait_time_thresholds']['emergency_critical']}min",
                'details': {
                    'avg_wait_time': round(emergency_critical['tempo_espera_minutos'].mean(), 1),
                    'max_wait_time': emergency_critical['tempo_espera_minutos'].max(),
                    'affected_units': emergency_critical['id_unidade'].value_counts().to_dict()
                },
                'actions_required': [
                    "Ativar protocolo de emergência",
                    "Reforçar equipes nas unidades críticas",
                    "Revisar classificação de risco"
                ]
            })
        
        # Atendimentos gerais com espera > 2h
        general_wait_critical = df_atendimentos[
            df_atendimentos['tempo_espera_minutos'] > self.alert_config['wait_time_thresholds']['general_critical']
        ]
        
        if not general_wait_critical.empty:
            alerts.append({
                'type': 'GENERAL_WAIT_CRITICAL',
                'severity': 'WARNING',
                'title': '⏰ TEMPO DE ESPERA GERAL ELEVADO',
                'message': f"{len(general_wait_critical)} atendimentos com espera >2h",
                'details': {
                    'avg_wait_time': round(general_wait_critical['tempo_espera_minutos'].mean(), 1),
                    'by_type': general_wait_critical['tipo_atendimento'].value_counts().to_dict(),
                    'by_unit': general_wait_critical['id_unidade'].value_counts().head(5).to_dict()
                },
                'actions_required': [
                    "Revisar fluxo de atendimento",
                    "Considerar abertura de horários extras"
                ]
            })
        
        return alerts
    
    def send_slack_alert(self, alert: Dict) -> bool:
        """Envia alerta para Slack"""
        
        if not self.slack_webhook:
            logger.warning("Slack webhook não configurado")
            return False
        
        # Formatar mensagem Slack
        color = {
            'CRITICAL': '#FF0000',  # Vermelho
            'WARNING': '#FFA500',   # Laranja
            'INFO': '#0000FF'       # Azul
        }.get(alert['severity'], '#808080')
        
        slack_payload = {
            "attachments": [{
                "color": color,
                "title": alert['title'],
                "text": alert['message'],
                "fields": [
                    {
                        "title": "Severidade",
                        "value": alert['severity'],
                        "short": True
                    },
                    {
                        "title": "Timestamp",
                        "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "short": True
                    }
                ]
            }]
        }
        
        # Adicionar detalhes se disponíveis
        if 'details' in alert:
            details_text = ""
            for key, value in alert['details'].items():
                if isinstance(value, (int, float)):
                    details_text += f"• {key}: {value}\n"
                elif isinstance(value, list) and len(value) <= 3:
                    details_text += f"• {key}: {', '.join(map(str, value))}\n"
                    
            if details_text:
                slack_payload["attachments"][0]["fields"].append({
                    "title": "Detalhes",
                    "value": details_text,
                    "short": False
                })
        
        # Adicionar ações requeridas
        if 'actions_required' in alert:
            actions_text = "\n".join(f"• {action}" for action in alert['actions_required'][:3])
            slack_payload["attachments"][0]["fields"].append({
                "title": "Ações Requeridas",
                "value": actions_text,
                "short": False
            })
        
        try:
            response = requests.post(self.slack_webhook, json=slack_payload, timeout=10)
            if response.status_code == 200:
                logger.info(f"Alerta Slack enviado: {alert['title']}")
                return True
            else:
                logger.error(f"Erro Slack: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Erro ao enviar Slack: {e}")
            return False
    
    def send_email_alert(self, alert: Dict) -> bool:
        """Envia alerta por email"""
        
        if not MimeText or not MimeMultipart:
            logger.warning("Email não disponível nesta versão do Python")
            return False
            
        if not self.email_config['sender_email'] or not self.email_config['recipient_emails']:
            logger.warning("Configuração de email incompleta")
            return False
        
        try:
            # Criar mensagem
            msg = MimeMultipart()
            msg['From'] = self.email_config['sender_email']
            msg['To'] = ', '.join(self.email_config['recipient_emails'])
            msg['Subject'] = f"ALERTA SAÚDE RJ: {alert['title']}"
            
            # Corpo do email
            body = f"""
ALERTA DO SISTEMA DE MONITORAMENTO DE SAÚDE - RIO DE JANEIRO

{alert['title']}
Severidade: {alert['severity']}
Timestamp: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

DESCRIÇÃO:
{alert['message']}

DETALHES:
{json.dumps(alert.get('details', {}), indent=2, ensure_ascii=False)}

AÇÕES REQUERIDAS:
{chr(10).join(f"• {action}" for action in alert.get('actions_required', []))}

---
Sistema Automatizado de Monitoramento
Pipeline de Dados - Secretaria Municipal de Saúde RJ
            """
            
            msg.attach(MimeText(body, 'plain', 'utf-8'))
            
            # Enviar email
            server = smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port'])
            server.starttls()
            server.login(self.email_config['sender_email'], self.email_config['sender_password'])
            
            text = msg.as_string()
            server.sendmail(
                self.email_config['sender_email'], 
                self.email_config['recipient_emails'], 
                text
            )
            server.quit()
            
            logger.info(f"Email enviado: {alert['title']}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao enviar email: {e}")
            return False
    
    def process_pipeline_alerts(self, data_dir: str = "./data_output") -> List[Dict]:
        """Processa todos os alertas do pipeline de dados"""
        
        all_alerts = []
        
        try:
            # Analisar ocupação de leitos
            if os.path.exists(f"{data_dir}/leitos_disponibilidade.csv"):
                df_leitos = pd.read_csv(f"{data_dir}/leitos_disponibilidade.csv")
                occupancy_alerts = self.analyze_occupancy_alerts(df_leitos)
                all_alerts.extend(occupancy_alerts)
            
            # Analisar tempos de espera
            if os.path.exists(f"{data_dir}/atendimentos.csv"):
                df_atendimentos = pd.read_csv(f"{data_dir}/atendimentos.csv")
                wait_time_alerts = self.analyze_wait_time_alerts(df_atendimentos)
                all_alerts.extend(wait_time_alerts)
            
            # Enviar alertas
            for alert in all_alerts:
                logger.info(f"Processando alerta: {alert['title']}")
                
                # Enviar via Slack
                self.send_slack_alert(alert)
                
                # Enviar via Email (apenas críticos)
                if alert['severity'] == 'CRITICAL':
                    self.send_email_alert(alert)
            
            return all_alerts
            
        except Exception as e:
            logger.error(f"Erro ao processar alertas: {e}")
            return []
    
    def generate_alerts_dashboard(self, alerts: List[Dict]) -> str:
        """Gera dashboard HTML dos alertas"""
        
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Dashboard de Alertas - Saúde RJ</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .alert-critical {{ border-left: 5px solid #FF0000; background: #FFE6E6; padding: 15px; margin: 10px 0; }}
        .alert-warning {{ border-left: 5px solid #FFA500; background: #FFF4E6; padding: 15px; margin: 10px 0; }}
        .alert-info {{ border-left: 5px solid #0000FF; background: #E6F3FF; padding: 15px; margin: 10px 0; }}
        .timestamp {{ color: #666; font-size: 12px; }}
        .actions {{ background: #F0F0F0; padding: 10px; margin-top: 10px; }}
    </style>
</head>
<body>
    <h1>🚨 Dashboard de Alertas - Sistema de Saúde RJ</h1>
    <p class="timestamp">Última atualização: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
    
    <h2>Resumo de Alertas</h2>
    <p>📊 Total de alertas: {len(alerts)}</p>
    <p>🚨 Críticos: {len([a for a in alerts if a['severity'] == 'CRITICAL'])}</p>
    <p>⚠️ Atenção: {len([a for a in alerts if a['severity'] == 'WARNING'])}</p>
    
    <h2>Alertas Detalhados</h2>
        """
        
        for alert in alerts:
            severity_class = f"alert-{alert['severity'].lower()}"
            
            html_content += f"""
    <div class="{severity_class}">
        <h3>{alert['title']}</h3>
        <p><strong>Severidade:</strong> {alert['severity']}</p>
        <p>{alert['message']}</p>
        
        <div class="actions">
            <strong>Ações Requeridas:</strong>
            <ul>
                {''.join(f"<li>{action}</li>" for action in alert.get('actions_required', []))}
            </ul>
        </div>
    </div>
            """
        
        html_content += """
</body>
</html>
        """
        
        # Salvar dashboard
        dashboard_path = "dashboard/alerts_dashboard.html"
        os.makedirs("dashboard", exist_ok=True)
        
        with open(dashboard_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Dashboard de alertas salvo em {dashboard_path}")
        return dashboard_path


def main():
    """Executa sistema de alertas"""
    
    from dotenv import load_dotenv
    load_dotenv('.env', override=True)
    
    # Configurar logging
    logging.basicConfig(level=logging.INFO)
    
    # Executar sistema de alertas
    alerts_system = HealthAlertsSystem()
    
    print("🔍 Analisando dados para alertas...")
    alerts = alerts_system.process_pipeline_alerts()
    
    if alerts:
        print(f"\n🚨 {len(alerts)} alertas identificados:")
        for alert in alerts:
            print(f"   {alert['severity']}: {alert['title']}")
        
        # Gerar dashboard
        dashboard_path = alerts_system.generate_alerts_dashboard(alerts)
        print(f"\n📊 Dashboard gerado: {dashboard_path}")
        # Validação de envio de email para alertas críticos
        print("\n🔎 Validando envio de email para alertas críticos...")
        for alert in alerts:
            if alert['severity'] == 'CRITICAL':
                result = alerts_system.send_email_alert(alert)
                if result:
                    print(f"✅ Email enviado para: {alerts_system.email_config['recipient_emails']}")
                else:
                    print(f"❌ Falha ao enviar email para: {alerts_system.email_config['recipient_emails']}")
        
    else:
        print("✅ Nenhum alerta crítico identificado")


if __name__ == "__main__":
    main()