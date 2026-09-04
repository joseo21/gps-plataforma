# Crear la infraestructura a mano — VPC propia, aislada de GpsGate

Región: **US East (N. Virginia) us-east-1**. Todo va en una VPC nueva.
GpsGate no se toca en ningún paso.

Costo extra por aislar: **cero**, mientras no creemos NAT Gateway.

---

## 1. VPC nueva

**VPC → Create VPC**. Arriba, elegí **VPC and more** (no "VPC only") —
esa opción crea subnets, Internet Gateway y tablas de rutas de una vez.

| Campo | Valor |
|---|---|
| Name tag auto-generation | tildado, `gps` |
| IPv4 CIDR block | `10.20.0.0/16` |
| IPv6 CIDR block | No IPv6 CIDR block |
| Tenancy | Default |
| Number of Availability Zones | **2** |
| Number of public subnets | **2** |
| Number of private subnets | **2** |
| **NAT gateways** | **None** ← importante |
| VPC endpoints | **None** |
| Enable DNS hostnames | tildado |
| Enable DNS resolution | tildado |

`10.20.0.0/16` no se pisa con el `10.0.0.0/16` de GpsGate. No vamos a
conectarlas, pero es buena práctica igual.

**NAT gateways en None** ahorra unos $32 al mes por cada uno. No hacen
falta: el EC2 va en subnet público con IP elástica, y Aurora no necesita
salida a internet.

Create VPC. En el diagrama final vas a ver
`gps-subnet-public1-us-east-1a`, `...public2-us-east-1b`,
`...private1-us-east-1a` y `...private2-us-east-1b`.

Los públicos ya vienen con ruta al Internet Gateway; no hay que verificar
nada a mano.

## 2. Security groups

**EC2 → Security Groups → Create security group**, dos veces. Ojo de
elegir la VPC nueva en ambas, no la de GpsGate.

**`gps-app`**
- Description: `GpsSinergy aplicacion`
- VPC: la nueva (`gps-vpc`)
- Inbound rules → Add rule, cuatro veces:

| Type | Port | Source | Descripción |
|---|---|---|---|
| HTTP | 80 | Anywhere-IPv4 | redirección y certbot |
| HTTPS | 443 | Anywhere-IPv4 | frontend y API |
| Custom TCP | 5027 | Anywhere-IPv4 | equipos Teltonika |
| SSH | 22 | My IP | administración |

Outbound: dejá el `All traffic` que viene por defecto.

El 5027 abierto al mundo es a propósito: los equipos salen por IPs
móviles que cambian, no hay rango que filtrar. La defensa es la whitelist
de IMEI. El 5028 de comandos no se agrega — queda dentro de Docker.

**`gps-db`**
- Description: `GpsSinergy Aurora`
- VPC: la nueva
- Inbound: **una sola regla** → Type `PostgreSQL` (el puerto 5432 se llena
  solo), Source → Custom → escribí `gps-app` y seleccionalo de la lista.

Nunca `0.0.0.0/0` en esta.

## 3. Aurora PostgreSQL Serverless v2

**RDS → Databases → Create database**.

- Método: **Standard create**
- Engine type: **Aurora (PostgreSQL Compatible)**
- Engine version: la que venga por defecto
- Templates: **Dev/Test**

**Settings**

| Campo | Valor |
|---|---|
| DB cluster identifier | `gps-prod` |
| Master username | `gpsadmin` |
| Credentials management | Self managed |
| Master password | clave larga, guardala aparte |

**Cluster storage configuration**: Aurora Standard.

**Instance configuration**: **Serverless v2**
- Minimum ACUs: `0.5`
- Maximum ACUs: `16`

Esto es lo que te deja pasar de 100 a 2049 equipos sin redimensionar
nada. Sube y baja solo según la carga.

**Availability & durability**: *Don't create an Aurora Replica*.

**Connectivity**

| Campo | Valor |
|---|---|
| Compute resource | Don't connect to an EC2 compute resource |
| VPC | la nueva (`gps-vpc`) |
| DB subnet group | Create new (toma los dos privados) |
| Public access | **No** |
| VPC security group | Choose existing → `gps-db`, quitá `default` |

**Additional configuration** (hay que desplegarlo al final del todo)
- Initial database name: **`gpsdb`** ← si lo dejás vacío, no se crea la base
- Backup retention period: **14 days**
- Encryption: activada

Create database. Tarda unos 8 minutos.

Cuando termine: **RDS → Databases → `gps-prod`** (la fila del clúster, no
la de la instancia) → pestaña **Connectivity & security** → copiá el
**Endpoint** que dice *Writer*. Es lo que va en `DATABASE_URL`.

## 4. EC2 — paso a paso

**EC2 → Instances → Launch instances**.

**Name and tags**
- Name: `gps-app`

**Application and OS Images**
- Buscá y elegí **Ubuntu**
- AMI: **Ubuntu Server 24.04 LTS (HVM), SSD Volume Type**
- Architecture: **64-bit (x86)**

**Instance type**
- **t3.small** (2 vCPU, 2 GiB)

**Key pair (login)**
- **Create new key pair**
- Name: `gps-key`
- Type: RSA
- Format: **.pem**
- Create key pair → se descarga sola. **Guardala bien, no se puede
  volver a descargar.**

**Network settings** → clic en **Edit** (esto es lo importante):

| Campo | Valor |
|---|---|
| VPC | la nueva (`gps-vpc`) |
| Subnet | uno de los **públicos**, ej. `gps-subnet-public1-us-east-1a` |
| Auto-assign public IP | **Enable** |
| Firewall | **Select existing security group** |
| Common security groups | `gps-app` |

Si dejás el subnet privado o el IP público en Disable, la instancia no va
a ser alcanzable ni va a poder bajar paquetes.

**Configure storage**
- 40 GiB, **gp3**

**Advanced details**: no toques nada.

En el panel derecho, **Launch instance**. Después "View all instances" y
esperá a que el Status check quede en verde, un par de minutos.

## 5. IP elástica

Sin esto, la IP pública cambia cada vez que reinicies la instancia, y
perdés todos los equipos hasta reconfigurarlos.

**EC2 → Elastic IPs → Allocate Elastic IP address** → Allocate.

Seleccioná la IP nueva → **Actions → Associate Elastic IP address**:
- Resource type: Instance
- Instance: `gps-app`
- Associate

Anotá la IP. **No va en los equipos**, va solo en el DNS.

## 6. Conectarte

En tu PC, donde bajaste la llave:

```bash
chmod 400 gps-key.pem
ssh -i gps-key.pem ubuntu@LA_IP_ELASTICA
```

En Windows PowerShell no existe `chmod`: clic derecho en el .pem →
Propiedades → Seguridad → Opciones avanzadas → Deshabilitar herencia →
dejá solo tu usuario con permisos.

## 7. DNS

Registro A de `gps.sinergychile.cl` → la IP elástica, TTL 60.

Los Teltonika se configuran **siempre con el dominio, nunca con la IP**.
Es la decisión que después te deja migrar a un Network Load Balancer
cambiando un registro DNS, en lugar de reconfigurar 2049 equipos a mano.

## 8. Alarmas

**SNS → Topics → Create topic** → Standard → `gps-alertas` → Create.
Luego **Create subscription** → Protocol: Email → tu correo. Confirmá
desde el mail que llega.

**CloudWatch → Alarms → Create alarm**, tres veces:

| Métrica | Umbral |
|---|---|
| EC2 `CPUUtilization` de `gps-app` | > 80% durante 15 min |
| RDS `ServerlessDatabaseCapacity` de `gps-prod` | > 14 ACU |
| RDS `DatabaseConnections` de `gps-prod` | > 80% del máximo |

Todas con acción → tema `gps-alertas`.

---

## Qué queda creado

| Recurso | Nombre | Costo aprox. |
|---|---|---|
| VPC + subnets + IGW | `gps-vpc` | $0 |
| EC2 t3.small + 40 GB gp3 | `gps-app` | ~$21 |
| IP elástica | — | ~$4 |
| Aurora Serverless v2 | `gps-prod` | ~$45-70 |
| SNS + CloudWatch | — | ~$1 |
| | **Total** | **~$75/mes** |

Nada de esto toca la VPC, los security groups ni la base de GpsGate. Son
dos mundos separados: si rompés algo acá, GpsGate ni se entera.

Con la IP elástica y el endpoint del writer en la mano, seguís con los
bloques de SSH.
