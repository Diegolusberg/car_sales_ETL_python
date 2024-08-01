import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Numeric, Date, ForeignKey, Index
from sqlalchemy.orm import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from io import StringIO
import logging

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Ruta al archivo CSV
file_path = 'CarSales-new.csv'

# Leer el archivo como texto
with open(file_path, 'r') as file:
    content = file.read()

# Reemplazar comillas alrededor del encabezado si es necesario
content = content.replace('"', '')

# Leer el contenido del archivo usando StringIO
df = pd.read_csv(StringIO(content), delimiter=',')

# Transformaciones de los datos de fechas
df['date'] = pd.to_datetime(df['Date'])
df['year'] = df['date'].dt.year
df['month'] = df['date'].dt.month
df['day'] = df['date'].dt.day
df['quarter'] = df['date'].dt.quarter

#Limpieza de espacios en blanco
df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

#Transformaciones de texto
df['Customer_Name'] = df['Customer_Name'].str.upper()
df['Gender'] = df['Gender'].replace({'Male': 'M', 'Female': 'F'})

#Transformacion de agregacion
df['income_category'] = pd.cut(df['Annual_Income'], bins=[0, 20000, 80000, 200000, 500000, 20000000], labels=['Low', 'Medium', 'High', 'Very High', 'Top'])
df['activity_type'] = 'PURCHASE'  # agregar una columna de tipo de actividad

# Importar la nueva base de datos declarativa
Base = declarative_base()

class DimTime(Base):
    __tablename__ = 'dim_time'
    date_id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, unique=True)
    year = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)
    quarter = Column(Integer)

    __table_args__ = (
        Index('idx_date', 'date'),
    )

class DimCustomer(Base):
    __tablename__ = 'dim_customer'
    customer_id = Column(Integer, primary_key=True, autoincrement=True)
    customer_name = Column(String(255))
    document_number = Column(Integer)
    gender = Column(String(1))
    annual_income = Column(Numeric)
    income_category = Column(String(255))

    __table_args__ = (
        Index('idx_document_number', 'document_number'),
    )

class DimCar(Base):
    __tablename__ = 'dim_car'
    car_id = Column(Integer, primary_key=True, autoincrement=True)
    car_ident = Column(String(255))
    company = Column(String(255))
    model = Column(String(255))
    engine = Column(String(255))
    transmission = Column(String(50))
    color = Column(String(50))
    body_style = Column(String(50))

    __table_args__ = (
        Index('idx_car_ident', 'car_ident'),
    )

class DimDealer(Base):
    __tablename__ = 'dim_dealer'
    dealer_no = Column(Integer, primary_key=True, autoincrement=True)
    dealer_name = Column(String(255))
    dealer_region = Column(String(255))

    __table_args__ = (
        Index('idx_dealer_name_region', 'dealer_name', 'dealer_region'),
    )

class FactSales(Base):
    __tablename__ = 'fact_sales'
    sale_id = Column(Integer, primary_key=True, autoincrement=True)
    car_id = Column(Integer, ForeignKey('dim_car.car_id'))
    date_id = Column(Integer, ForeignKey('dim_time.date_id'))
    customer_id = Column(Integer, ForeignKey('dim_customer.customer_id'))
    dealer_no = Column(Integer, ForeignKey('dim_dealer.dealer_no'))
    price = Column(Numeric)

    __table_args__ = (
        Index('idx_fact_sales', 'car_id', 'date_id', 'customer_id', 'dealer_no'),
    )

class FactCustomerActivity(Base):
    __tablename__ = 'fact_customer_activity'
    activity_cust_id = Column(Integer, primary_key=True, autoincrement=True)
    customer_name = Column(String(255))
    document_number = Column(Integer)
    gender = Column(String(10))
    annual_income = Column(Numeric)
    date_id = Column(Integer,  ForeignKey('dim_time.date_id'))
    dealer_no = Column(Integer, ForeignKey('dim_dealer.dealer_no'))
    car_id = Column(Integer, ForeignKey('dim_car.car_id'))
    activity_type = Column(String(50))

    __table_args__ = (
        Index('idx_fact_customer_activity', 'document_number'),
    )

class FactDealers(Base):
    __tablename__ = 'fact_dealers'
    dealer_id = Column(Integer, primary_key=True, autoincrement=True)
    dealer_name = Column(String(255))
    dealer_region = Column(String(255))
    date_id = Column(Integer,  ForeignKey('dim_time.date_id'))
    car_id = Column(Integer, ForeignKey('dim_car.car_id'))
    customer_id = Column(Integer, ForeignKey('dim_customer.customer_id'))

    __table_args__ = (
        Index('idx_fact_dealers_name_region', 'dealer_name', 'dealer_region'),
    )       

class FactCars(Base):
    __tablename__ = 'fact_cars'
    car_id = Column(Integer, primary_key=True, autoincrement=True)
    car_ident = Column(String(255))
    company = Column(String(255))
    model = Column(String(255))
    engine = Column(String(255))
    transmission = Column(String(50))
    color = Column(String(50))
    body_style = Column(String(50))
    customer_id = Column(Integer, ForeignKey('dim_customer.customer_id'))
    date_id = Column(Integer,  ForeignKey('dim_time.date_id'))
    dealer_no = Column(Integer, ForeignKey('dim_dealer.dealer_no'))

    __table_args__ = (
        Index('idx_fact_cars_ident', 'car_ident'),
        Index('idx_fact_cars_modelCompany', 'company', 'model'),
    )

# Crear la conexión a la base de datos PostgreSQL
DATABASE_URI = 'postgresql+psycopg2://postgres:admin@localhost:5432/cars_sales_datawarehouse'
engine = create_engine(DATABASE_URI)

# Crear las tablas
def create_tables(engine):
    print("Creando tablas...")
    try:
         # Eliminar todas las tablas
        Base.metadata.drop_all(engine)

        # Crear todas las tablas
        Base.metadata.create_all(engine)
        print("Tablas creadas correctamente.")
    except SQLAlchemyError as e:
        print(f"Error al crear las tablas: {e}")

# Insertar datos en las tablas de dimensiones
def insert_data_init(engine, df):
    with engine.connect() as conn:
         # Insertar datos en la tabla Dim_Time
        dim_time = df[['date', 'year', 'month', 'day', 'quarter']].drop_duplicates()
        dim_time.to_sql('dim_time', engine, if_exists='append', index=False)

        # Insertar datos en la tabla Dim_Customer
        dim_customer = df[['Customer_Name', 'Document_Number', 'Gender', 'Annual_Income', 'income_category']].drop_duplicates()
        dim_customer.columns = ['customer_name', 'document_number', 'gender', 'annual_income', 'income_category']
        dim_customer.to_sql('dim_customer', engine, if_exists='append', index=False)

        # Insertar datos en la tabla Dim_Car
        dim_car = df[['Car_Ident', 'Company', 'Model', 'Engine', 'Transmission', 'Color', 'Body_Style']].drop_duplicates()
        dim_car.columns = ['car_ident', 'company', 'model', 'engine', 'transmission', 'color', 'body_style']
        dim_car.to_sql('dim_car', engine, if_exists='append', index=False)

        # Insertar datos en la tabla Dim_Dealer
        dim_dealer = df[['Dealer_Name', 'Dealer_Region']].drop_duplicates()
        dim_dealer.columns = ['dealer_name', 'dealer_region']
        dim_dealer.to_sql('dim_dealer', engine, if_exists='append', index=False)

        # Preparar datos para la tabla Fact_Sales
        fact_sales = df[['Date', 'Customer_Name', 'Document_Number', 'Car_Ident', 'Dealer_Name', 'Dealer_Region', 'PriceUSD']]
        fact_sales.columns = ['date', 'customer_name', 'document_number', 'car_ident', 'dealer_name', 'dealer_region', 'price']

        # Mapear los nombres a los IDs correspondientes de las dimensiones
        customer_map = pd.read_sql('SELECT customer_id, document_number FROM dim_customer', conn)
        fact_sales = fact_sales.merge(customer_map, on='document_number', how='left')

        car_map = pd.read_sql('SELECT car_id, car_ident FROM dim_car', conn)
        fact_sales = fact_sales.merge(car_map, on=['car_ident'], how='left')

        dealer_map = pd.read_sql('SELECT dealer_no, dealer_name, dealer_region FROM dim_dealer', conn)
        fact_sales = fact_sales.merge(dealer_map, on=['dealer_name', 'dealer_region'], how='left')    

        # Mapear Date a Date_ID
        time_map = pd.read_sql('SELECT date_id, date FROM dim_time', conn)
        fact_sales['date'] = pd.to_datetime(fact_sales['date'])
        time_map['date'] = pd.to_datetime(time_map['date'])
        fact_sales = fact_sales.merge(time_map, on='date', how='left')

        # Seleccionar y renombrar columnas para insertar en Fact_Sales
        fact_sales = fact_sales[['date_id', 'car_id', 'customer_id', 'dealer_no', 'price']]

        # Eliminar filas con valores NaN resultantes del merge
        fact_sales.dropna(inplace=True)
    
        # Dividir el DataFrame en lotes
        batch_size = 1000
        for start in range(0, len(fact_sales), batch_size):
            end = start + batch_size
            fact_sales_batch = fact_sales[start:end]
    
            # Insertar el lote en la tabla fact_sales
            fact_sales_batch.to_sql('fact_sales', engine, if_exists='append', index=False)

            # Imprimir el progreso
            print(f'Inserted rows {start} to {end}')

    print("Datos iniciales insertados correctamente en el Data Warehouse")

# Insertar datos en la tabla de hechos de actividades de clientes
def insert_customer_activity(engine, df):
    with engine.connect() as conn:
        
        # Preparar datos para la tabla fact_customer_activity
        fact_customer_activity = df[['Document_Number', 'Customer_Name',  'Gender', 'Annual_Income', 'Date', 'Dealer_Name', 'Dealer_Region', 'Car_Ident', 'activity_type']]
        fact_customer_activity.columns = ['document_number', 'customer_name',  'gender', 'annual_income', 'date', 'dealer_name', 'dealer_region', 'car_ident', 'activity_type']

         # Mapear dim_car
        car_map = pd.read_sql('SELECT car_id, car_ident FROM dim_car', conn)
        fact_customer_activity = fact_customer_activity.merge(car_map, on=['car_ident'], how='left')

        dealer_map = pd.read_sql('SELECT dealer_no, dealer_name, dealer_region FROM dim_dealer', conn)
        fact_customer_activity = fact_customer_activity.merge(dealer_map, on=['dealer_name', 'dealer_region'], how='left')  

         # Mapear Date a Date_ID
        time_map = pd.read_sql('SELECT date_id, date FROM dim_time', conn)
        fact_customer_activity['date'] = pd.to_datetime(fact_customer_activity['date'])
        time_map['date'] = pd.to_datetime(time_map['date'])
        fact_customer_activity = fact_customer_activity.merge(time_map, on='date', how='left')
        

        # Seleccionar y renombrar columnas para insertar en fact_customer_activity
        fact_customer_activity = fact_customer_activity[['document_number', 'customer_name',  'gender', 'annual_income', 'date_id', 'dealer_no', 'car_id', 'activity_type']]

        # Eliminar filas con valores NaN resultantes del merge
        fact_customer_activity.dropna(inplace=True)

        # Insertar el lote en la tabla fact_sales
        fact_customer_activity.to_sql('fact_customer_activity', engine, if_exists='append', index=False)

        print("Tabla de hechos de clientes insertada correctamente en el Data Warehouse")

#Insertar tabla de hechos de Proveedores
def insert_dealer_table(engine, df):
    with engine.connect() as conn:
        
        fact_dealers = df[['Document_Number', 'Date', 'Dealer_Name', 'Dealer_Region', 'Car_Ident']]
        fact_dealers.columns = ['document_number', 'date', 'dealer_name', 'dealer_region', 'car_ident']

        # Mapear dim_customer
        customer_map = pd.read_sql('SELECT customer_id, document_number FROM dim_customer', conn)
        fact_dealers = fact_dealers.merge(customer_map, on='document_number', how='left')
        
        # Mapear dim_car
        car_map = pd.read_sql('SELECT car_id, car_ident FROM dim_car', conn)
        fact_dealers = fact_dealers.merge(car_map, on=['car_ident'], how='left')

         # Mapear Date a Date_ID
        time_map = pd.read_sql('SELECT date_id, date FROM dim_time', conn)
        fact_dealers['date'] = pd.to_datetime(fact_dealers['date'])
        time_map['date'] = pd.to_datetime(time_map['date'])
        fact_dealers = fact_dealers.merge(time_map, on='date', how='left')
        

        # Seleccionar y renombrar columnas para insertar en fact_dealer
        fact_dealers = fact_dealers[['dealer_name', 'dealer_region', 'date_id', 'car_id','customer_id']]

        # Eliminar filas con valores NaN resultantes del merge
        fact_dealers.dropna(inplace=True)

        # Insertar el lote en la tabla fact_dealer
        fact_dealers.to_sql('fact_dealers', engine, if_exists='append', index=False)

        print("Tabla de hechos de proveedores insertada correctamente en el Data Warehouse")              

#Insertar tabla de hechos de Proveedores
def insert_cars_table(engine, df):
    with engine.connect() as conn:
        
        fact_cars = df[['Car_Ident', 'Company', 'Model', 'Engine', 'Transmission', 'Color', 'Body_Style', 'Document_Number', 'Dealer_Name', 'Dealer_Region', 'Date']]
        fact_cars.columns = ['car_ident', 'company', 'model', 'engine', 'transmission', 'color', 'body_style', 'document_number', 'dealer_name', 'dealer_region', 'date']

        # Mapear dim_customer
        customer_map = pd.read_sql('SELECT customer_id, document_number FROM dim_customer', conn)
        fact_cars = fact_cars.merge(customer_map, on='document_number', how='left')

        # Mapear dim_customer
        dealer_map = pd.read_sql('SELECT dealer_no, dealer_name, dealer_region FROM dim_dealer', conn)
        fact_cars = fact_cars.merge(dealer_map, on=['dealer_name', 'dealer_region'], how='left')         

         # Mapear Date a Date_ID
        time_map = pd.read_sql('SELECT date_id, date FROM dim_time', conn)
        fact_cars['date'] = pd.to_datetime(fact_cars['date'])
        time_map['date'] = pd.to_datetime(time_map['date'])
        fact_cars = fact_cars.merge(time_map, on='date', how='left')
        

        # Seleccionar y renombrar columnas para insertar en fact_cars
        fact_cars = fact_cars[['car_ident', 'company', 'model', 'engine', 'transmission', 'color', 'body_style', 'customer_id', 'date_id', 'dealer_no']]

        # Eliminar filas con valores NaN resultantes del merge
        fact_cars.dropna(inplace=True)

        # Insertar el lote en la tabla fact_dealer
        fact_cars.to_sql('fact_cars', engine, if_exists='append', index=False)

        print("Tabla de hechos de autos insertada correctamente en el Data Warehouse")    
            
# Crear las tablas y cargar los datos
create_tables(engine)
insert_data_init(engine, df)
insert_customer_activity(engine, df)
insert_dealer_table(engine, df)
insert_cars_table(engine, df)




