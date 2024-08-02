/*tabla de hechos ventas*/
--ventas totales por año y mes en usd
select 
    t.year,
    t.month,
    sum(f.price) as total_sales
from 
    fact_sales f
join 
    dim_time t on f.date_id = t.date_id
group by 
    t.year, t.month
order by 
    t.year, t.month;

--ventas totales por marca y modelo de coche
select 
    c.company,
    c.model,
    sum(f.price) as total_sales
from 
    fact_sales f
join 
    dim_car c on f.car_id = c.car_id
group by 
    c.company, c.model
order by 
    total_sales desc;


--recuento de ventas por región del concesionario
select 
    d.dealer_region,
    count(f.customer_id) as count_sales
from 
    fact_sales f
join 
    dim_dealer d on f.dealer_no = d.dealer_no
group by 
    d.dealer_region
order by 
    total_sales desc;

--cantidad de ventas por categorias de clientes
select 
     c.income_category,
      cast(count(f.sale_id) as integer) as sales
from 
    fact_sales f
join 
    dim_customer c on f.customer_id = c.customer_id
group by 
    c.income_category
order by 
    sales desc;

/*tabla de hechos proveedores*/    
--cantidad de proveedores por region
select  count(1) as cantidad,
		x.dealer_region
  from fact_dealers x
where 1=1
group by x.dealer_region

--cantidad de clientes por genero para cada proveedor
select  x.dealer_region,
		y.gender,
		count(x.customer_id)
  from fact_dealers x, dim_customer y
where x.customer_id = y.customer_id
group by x.dealer_region, y.gender
order by x.dealer_region, y.gender 	


/*tabla de hechos clientes*/    
--promedio de ingresos anuales por genero
select
	case
		when gender = 'm' then 'masculino'
		else 'femenino'
	end gender,
	cast(avg(annual_income) as integer) as avg_income
from
	fact_customer_activity
group by
	gender
--cantidad de compras por año
select
	y.year,
	x.activity_type,
	count(activity_cust_id) as cantidad
from
	fact_customer_activity x, dim_time y
where x.date_id = y.date_id
group by y.year,
	x.activity_type

/*tabla de hechos autos*/        
--cantidad de autos vendidos por compañia
SELECT x.company,
	   count(1)
 FROM fact_cars x
group by x.company

--cantidad de autos vendidos por compañia por año, por mes
SELECT x.company,
		y.year,
		y.month,
	   count(1)
 FROM fact_cars x, DIM_TIME Y
where x.date_id = y.date_id
group by x.company,
		y.year,
		y.month	
order by x.company,
		y.year,
		y.month

--compañias que vendieron mas de 500 autos en el año por año
SELECT x.company,
		y.year,
	   count(1)
 FROM fact_cars x, DIM_TIME Y
where x.date_id = y.date_id
group by x.company,
		y.year	
having count(1) > 500	
order by x.company,
		y.year



