# 🚀 Nexus Migration Tool 
Herramienta interactiva para migrar datos del monolito a microservicios para Nexus.

## 🛠️ Instalación

```bash
# Instalar dependencias
pip install -r requirements.txt

# Configurar entorno
cp .env.example .env
# Editar .env con tus URLs de base de datos
```

## 🚀 Uso

### Solo Modo Interactivo

```bash
python app.py
```

Y ya está. La aplicación te guía paso a paso con menús intuitivos.

## 🎨 Interfaz

```
                    ███▄    █ ▓█████ ▒██   ██▒ █    ██   ██████ 
                    ██ ▀█   █ ▓█   ▀ ▒▒ █ █ ▒░ ██  ▓██▒▒██    ▒ 
                   ▓██  ▀█ ██▒▒███   ░░  █   ░▓██  ▒██░░ ▓██▄   
                   ▓██▒  ▐▌██▒▒▓█  ▄  ░ █ █ ▒ ▓▓█  ░██░  ▒   ██▒
                   ▒██░   ▓██░░▒████▒▒██▒ ▒██▒▒▒█████▓ ▒██████▒▒
                   ░ ▒░   ▒ ▒ ░░ ▒░ ░▒▒ ░ ░▓ ░░▒▓▒ ▒ ▒ ▒ ▒▓▒ ▒ ░
                   ░ ░░   ░ ▒░ ░ ░  ░░░   ░▒ ░░░▒░ ░ ░ ░ ░▒  ░ ░
                      ░   ░ ░    ░    ░    ░   ░░░ ░ ░ ░  ░  ░  
                            ░    ░  ░ ░    ░     ░           ░  

                              🚀 Migration Tool

┏━━━━┳━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ ID ┃ Módulo             ┃ Submódulos                           ┃
┡━━━━╇━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ 1  │ ms-users           │ roles-views, users                   │
│ 2  │ ms-payments        │ payment-configs, payments            │
│ 3  │ ms-membership      │ membership-plans, memberships        │
│ 4  │ ms-points          │ user-points, weekly-volumes          │
│ 5  │ 🚪 Salir           │ Cerrar aplicación                    │
└────┴────────────────────┴──────────────────────────────────────┘

🎯 Selecciona un módulo (1-5): 
```

## 📋 Orden de Migración

1. `ms-users` → `roles-views` (base)
2. `ms-users` → `users` 
3. `ms-payments` → `payment-configs`
4. `ms-payments` → `payments`
5. `ms-membership` → `membership-plans`
6. `ms-membership` → `memberships`
7. `ms-points` → `user-points`
8. `ms-points` → `weekly-volumes`

## 🔧 Variables de Entorno

```bash
# .env
NEXUS_POSTGRES_URL=postgresql://user:pass@host:port/nexus_db
MS_NEXUS_USER=mongodb://user:pass@host:port/ms_nexus_user
MS_NEXUS_PAYMENTS=postgresql://user:pass@host:port/ms_nexus_payments
MS_NEXUS_MEMBERSHIP=postgresql://user:pass@host:port/ms_nexus_membership
MS_NEXUS_POINTS=postgresql://user:pass@host:port/ms_nexus_points
```

