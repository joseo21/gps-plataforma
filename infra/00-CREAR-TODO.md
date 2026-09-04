# Crear todo desde cero — GpsSinergy

Todos los bloques de AWS se pegan en **CloudShell** (el ícono de terminal
abajo a la izquierda en la consola). Viene con AWS CLI ya autenticado, no
hay que instalar ni configurar nada.

Objetivo de la fase 1: unos $90/mes con 100 equipos. El diseño soporta
2049 equipos a 15 s sin cambiar de arquitectura — solo subir el techo de
ACU y agregar un ingestor.

---

## 1. Variables (CloudShell)

Editá el dominio y la clave antes de pegar.

```bash
export REGION=us-east-1
export DOMINIO=gps.sinergychile.cl
export DB_PASS='CambiaEstaClaveLargaYSegura2026'
export PROY=gps

export VPC=$(aws ec2 describe-vpcs --region $REGION \
  --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text)
export SUBNETS=$(aws ec2 describe-subnets --region $REGION \
  --filters Name=vpc-id,Values=$VPC --query 'Subnets[].SubnetId' --output text)
export SUBNET1=$(echo $SUBNETS | cut -d' ' -f1)

echo "VPC=$VPC"; echo "SUBNETS=$SUBNETS"
```

## 2. Security groups

```bash
export SG_APP=$(aws ec2 create-security-group --region $REGION \
  --group-name $PROY-app --description "GpsSinergy app" --vpc-id $VPC \
  --query GroupId --output text)

export SG_DB=$(aws ec2 create-security-group --region $REGION \
  --group-name $PROY-db --description "GpsSinergy Aurora" --vpc-id $VPC \
  --query GroupId --output text)

export MI_IP=$(curl -s https://checkip.amazonaws.com)

# Puerto 5027: abierto al mundo. Los equipos salen por IPs moviles que
# cambian, no hay rango que filtrar. La defensa es la whitelist de IMEI.
for P in 80 443 5027; do
  aws ec2 authorize-security-group-ingress --region $REGION \
    --group-id $SG_APP --protocol tcp --port $P --cidr 0.0.0.0/0 >/dev/null
done
aws ec2 authorize-security-group-ingress --region $REGION \
  --group-id $SG_APP --protocol tcp --port 22 --cidr $MI_IP/32 >/dev/null

# Aurora: solo desde el EC2. Nunca 0.0.0.0/0.
aws ec2 authorize-security-group-ingress --region $REGION \
  --group-id $SG_DB --protocol tcp --port 5432 --source-group $SG_APP >/dev/null

echo "SG_APP=$SG_APP"; echo "SG_DB=$SG_DB"
```

## 3. Aurora PostgreSQL Serverless v2

Serverless v2 escala de 0,5 a 16 ACU sola, sin reiniciar. Por eso pasar
de 100 a 2049 equipos no requiere redimensionar nada.

```bash
aws rds create-db-subnet-group --region $REGION \
  --db-subnet-group-name $PROY-subnets \
  --db-subnet-group-description "GpsSinergy" \
  --subnet-ids $SUBNETS >/dev/null

aws rds create-db-cluster --region $REGION \
  --db-cluster-identifier $PROY-prod \
  --engine aurora-postgresql \
  --master-username gpsadmin --master-user-password "$DB_PASS" \
  --database-name gpsdb \
  --db-subnet-group-name $PROY-subnets \
  --vpc-security-group-ids $SG_DB \
  --serverless-v2-scaling-configuration MinCapacity=0.5,MaxCapacity=16 \
  --backup-retention-period 14 \
  --storage-encrypted \
  --no-publicly-accessible >/dev/null

aws rds create-db-instance --region $REGION \
  --db-instance-identifier $PROY-prod-1 \
  --db-cluster-identifier $PROY-prod \
  --engine aurora-postgresql \
  --db-instance-class db.serverless >/dev/null

echo "Creando cluster, tarda unos 8 minutos..."
aws rds wait db-instance-available --region $REGION \
  --db-instance-identifier $PROY-prod-1

export DB_HOST=$(aws rds describe-db-clusters --region $REGION \
  --db-cluster-identifier $PROY-prod --query 'DBClusters[0].Endpoint' --output text)
echo "DB_HOST=$DB_HOST"
```

> Empezá con almacenamiento Standard. Cuando pases de ~800 equipos, mirá
> en Cost Explorer si `RDS:StorageIOUsage` supera el 25% del gasto de
> Aurora; si lo supera, pasá a I/O-Optimized (`--storage-type aurora-iopt1`)
> con `modify-db-cluster`, sin migrar datos.

## 4. EC2 con IP elástica

```bash
export AMI=$(aws ssm get-parameter --region $REGION \
  --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
  --query Parameter.Value --output text)

aws ec2 create-key-pair --region $REGION --key-name $PROY-key \
  --query KeyMaterial --output text > ~/$PROY-key.pem
chmod 400 ~/$PROY-key.pem

export EC2=$(aws ec2 run-instances --region $REGION \
  --image-id $AMI --instance-type t3.small \
  --key-name $PROY-key --security-group-ids $SG_APP --subnet-id $SUBNET1 \
  --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=40,VolumeType=gp3}' \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$PROY-app}]" \
  --query 'Instances[0].InstanceId' --output text)

aws ec2 wait instance-running --region $REGION --instance-ids $EC2

export ALLOC=$(aws ec2 allocate-address --region $REGION --domain vpc \
  --query AllocationId --output text)
aws ec2 associate-address --region $REGION \
  --instance-id $EC2 --allocation-id $ALLOC >/dev/null

export IP=$(aws ec2 describe-addresses --region $REGION \
  --allocation-ids $ALLOC --query 'Addresses[0].PublicIp' --output text)

echo "==============================="
echo "IP ELASTICA: $IP"
echo "DB_HOST:     $DB_HOST"
echo "Llave:       ~/$PROY-key.pem  (descargala de CloudShell: Actions > Download file)"
echo "==============================="
```

Bajá la llave con **Actions → Download file** en CloudShell, ruta
`/home/cloudshell-user/gps-key.pem`.

## 5. DNS

Creá un registro A de `gps.sinergychile.cl` apuntando a esa IP elástica,
con TTL 60.

**Configurá los equipos siempre con el dominio, nunca con la IP.** Es la
decisión que después te permite migrar a un Network Load Balancer
cambiando un registro DNS, en vez de reconfigurar 2049 equipos a mano.

## 6. Preparar el servidor

```bash
ssh -i ~/gps-key.pem ubuntu@LA_IP
```

Una vez adentro:

```bash
sudo apt update && sudo apt install -y docker.io docker-compose-v2 git postgresql-client
sudo usermod -aG docker ubuntu
sudo mkdir -p /opt/gps && sudo chown -R ubuntu:ubuntu /opt/gps

sudo fallocate -l 4G /swapfile && sudo chmod 600 /swapfile
sudo mkswap /swapfile && sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

Desconectá y volvé a entrar para que tome el grupo `docker`.

## 7. Subir el código (desde tu PC)

```bash
scp -i ~/gps-key.pem -r ./gps-stack/. ubuntu@LA_IP:/opt/gps/
scp -i ~/gps-key.pem ./server.py ubuntu@LA_IP:/opt/gps/ingestor/server.py
```

## 8. Configurar y arrancar (en el SSH)

```bash
cd /opt/gps
git clone https://github.com/Sebastian1841/GpsSinergy frontend/src-repo

cat > .env <<'EOF'
DATABASE_URL=postgresql://gpsadmin:TU_CLAVE@TU_DB_HOST:5432/gpsdb
PUBLIC_ORIGIN=https://gps.sinergychile.cl
EOF

export $(grep DATABASE_URL .env)
psql "$DATABASE_URL" -f db/schema.sql
psql "$DATABASE_URL" -c "SELECT tablename FROM pg_tables WHERE tablename LIKE 'telemetry_%';"

mkdir -p certbot/conf certbot/www
docker run --rm -p 80:80 -v $PWD/certbot/conf:/etc/letsencrypt \
  certbot/certbot certonly --standalone -d gps.sinergychile.cl \
  --agree-tos -m informatica@sinergygroup.cl -n

docker compose up -d --build
docker compose ps
curl -s http://localhost/api/health; echo
```

## 9. Alarmas — no lo saltees

Tu GpsGate actual corre con 2049 equipos y **cero alarmas**. No repitas eso.

```bash
# En CloudShell
export TEMA=$(aws sns create-topic --region $REGION --name $PROY-alertas \
  --query TopicArn --output text)
aws sns subscribe --region $REGION --topic-arn $TEMA \
  --protocol email --notification-endpoint informatica@sinergygroup.cl

aws cloudwatch put-metric-alarm --region $REGION \
  --alarm-name $PROY-cpu-ec2 --alarm-actions $TEMA \
  --metric-name CPUUtilization --namespace AWS/EC2 --statistic Average \
  --period 300 --evaluation-periods 3 --threshold 80 \
  --comparison-operator GreaterThanThreshold \
  --dimensions Name=InstanceId,Value=$EC2

aws cloudwatch put-metric-alarm --region $REGION \
  --alarm-name $PROY-acu-aurora --alarm-actions $TEMA \
  --metric-name ServerlessDatabaseCapacity --namespace AWS/RDS --statistic Average \
  --period 300 --evaluation-periods 3 --threshold 14 \
  --comparison-operator GreaterThanThreshold \
  --dimensions Name=DBClusterIdentifier,Value=$PROY-prod
```

Confirmá la suscripción en tu correo.

Y la alarma que más vale, que no es de infraestructura: cuántos equipos
llevan más de 2 horas sin reportar.

```sql
SELECT count(*) FROM devices d
LEFT JOIN device_state s ON s.device_id = d.id
WHERE d.activo AND (s.ts IS NULL OR s.ts < now() - interval '2 hours');
```

## 10. Equipos y verificación

```sql
INSERT INTO devices (tenant_id, imei, nombre, patente, modelo)
VALUES (1, '861585041880343', 'Camion 1', 'ABCD-12', 'FMB920');
```

En el Teltonika Configurator, GPRS → Server Settings: dominio
`gps.sinergychile.cl`, puerto `5027`, protocolo TCP.

Después de unos días con datos reales, la comprobación que importa:

```sql
SELECT pg_size_pretty(pg_total_relation_size('telemetry')) AS total,
       count(*) AS filas,
       pg_total_relation_size('telemetry') / NULLIF(count(*),0) AS bytes_fila
FROM telemetry;
```

Objetivo: 120 a 160 bytes por fila. Si da más de 250, hay algún IO
cayendo en `io_extra` en cada registro que debería ser columna:

```sql
SELECT k, count(*) FROM telemetry, jsonb_object_keys(io_extra) k
WHERE ts > now() - interval '1 hour' GROUP BY 1 ORDER BY 2 DESC;
```

---

## Archivado — obligatorio, no opcional

Tu GpsGate mantiene el costo plano porque purga. Acá el equivalente,
mensual: desprender las particiones de más de 90 días, volcarlas a
Parquet en S3 y borrarlas.

```sql
SELECT * FROM particiones_a_archivar(90);
-- Por cada una:
ALTER TABLE telemetry DETACH PARTITION telemetry_2026_12;
-- exportar a S3 con aws_s3.query_export_to_s3 o pg_dump, y despues:
DROP TABLE telemetry_2026_12;
```

Sin esto, a 11,8 millones de registros diarios la base crece unos 45 GB
por mes y no para nunca.

## Escalar a 2049 equipos

Ninguno de estos pasos es reescribir código:

1. Subir `MaxCapacity` de Aurora de 16 a 32 ACU — sin reinicio
2. EC2 a `t3.large` o `m7g.large`
3. Redis local a ElastiCache
4. Cambiar el estado del ingestor de memoria a Redis (esto sí es código,
   pero hay que hacerlo antes de poner el segundo ingestor)
5. Network Load Balancer delante de dos ingestores, cambiando el DNS
6. Réplica de lectura de Aurora para los reportes
