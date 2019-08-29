from neo4j import GraphDatabase
import datetime


def get_vehicle_id(session):
    return session.run('match(n:vehicle) return max(n.id) as id').data()[0]['id'] + 1

def get_package_id(session):
    return session.run('match(n:package) return max(n.id) as id').data()[0]['id'] + 1


#Gestion de rutas

def get_efficinent_path(session, start_node, end_node, delivery_type):

    now = datetime.datetime.now()

    if delivery_type == 1:
        time_limit = datetime.datetime(now.year, now.month, now.day, 18)
        minutes_left = (time_limit - now).seconds/60
        if now > time_limit:
            return -1

    elif delivery_type == 2:
        time_limit = datetime.datetime(now.year, now.month, now.day, 5) + datetime.timedelta(days=1)
        minutes_left = (time_limit - now).seconds / 60

    elif delivery_type == 3:
        minutes_left = 10000

    else:
        print('Invalid delivery type')
        return -1

    query = '''
            MATCH p =(a:warehouse)-[*]-(b:delivery_point)
            WHERE a.name = {start} AND b.name = {finish}
            with p, reduce(total_cost = 0, r IN relationships(p) | total_cost + r.cost) AS total_cost,
            reduce(total_time = 0, r IN relationships(p) | total_time + r.time + r.load_time) AS total_time
            order by total_cost
            where total_time < {remainingTime}
            return p, total_time, total_cost
            limit 1
            '''
    path = session.run(query, start=start_node, finish=end_node, remainingTime=minutes_left)

    return path.data()


#Gestion de flota

def charter_vehicle(session, start_position, end_position, delivery_type, package_id):
    query = '''
            match (n {name:{location}})
            merge (v:vehicle {destination: {destination}, delivery_type: {delivery_type}})-[r:location]->(n)
            on create set v.id = {vehicle_id}, v.departure_time = timestamp(), r.date = timestamp()
            with v
            match (p:package {id: {package_id}})
            create (p)-[:transported]->(v)
            '''
    session.run(query, location=start_position, destination=end_position, delivery_type=delivery_type, vehicle_id=get_vehicle_id(session), package_id=package_id)


def update_position(driver, vehicle, position):
    query = '''
            match (v:vehicle)-[r:location]->(n)
            where v.id = {vehicle_id}
            delete r
            with v
            match (m)
            where m.name = {new_position}
            create (v)-[:location {date: timestamp()}]->(m)
            '''
    with driver.session() as session:
        session.run(query, vehicle_id=vehicle, new_position=position)


#Gestion de paquetes

def create_package(session, total_time, total_cost, delivery_type):
    query = '''
            create (n:package {id: {package_id}, total_time: {total_time},
            total_cost: {total_cost}, delivery_type: {delivery_type}})
            '''
    package_id = get_package_id(session)
    session.run(query, package_id=package_id, total_time=total_time, total_cost=total_cost, delivery_type=delivery_type)
    return package_id


def get_package_status(driver, package_id):
    query = '''
            match (p:package {id: {package_id}})-[t:transported]->(v:vehicle)-[l:location]->(n)
            return n as position,
            p.total_time -  duration.between(datetime({epochmillis:v.departure_time}),
            datetime({epochmillis:l.date})).minutes as time_left
            '''
    with driver.session() as session:
        status = session.run(query, package_id=package_id)

    return status.data()


#Gestion de proveedores

def assign_supplier(session, supplier_id, package_id):
    query = '''
            match (p:package {id:{package_id}}), (s:supplier {id:{supplier_id}})
            create (s)-[r:supplies {paid: false}]->(p)
            '''
    session.run(query, package_id=package_id, supplier_id=supplier_id)


def get_supplier_packages(driver, supplier_id, delivery_types):
    query = '''
            match (s:supplier {id: {supplier_id}})-[r:supplies]->(p:package)
            where p.delivery_type in {delivery_types}
            return p
            '''
    with driver.session() as session:
        packages = session.run(query, supplier_id=supplier_id, delivery_types=delivery_types)

    return packages.data()

def get_deliveries_report(driver, supplier_id):
    query = '''
            match (s:supplier {id: {supplier_id}})-[r:supplies {paid: {paid}}]->(p:package)
            with collect (p) as q
            unwind q as r
            return q, count(r), sum(r.total_cost)
            '''

    with driver.session() as session:
        paid = session.run(query, supplier_id=supplier_id, paid=True)
        not_paid = session.run(query, supplier_id=supplier_id, paid=False)

    results = {}
    results['paid'] = paid.data()
    results['not_paid'] = not_paid.data()

    return results

#Metodo para hacer un pedido llamando a los apartados anteriores
def create_delivery(driver, start_node, end_node, delivery_type, supplier_id):
    with driver.session() as session:
        path = get_efficinent_path(session, start_node, end_node, delivery_type)
        if path == -1 or path == []:
            return -1

        package_id = create_package(session, path[0]['total_time'], path[0]['total_cost'], delivery_type)
        assign_supplier(session, supplier_id, package_id)
        charter_vehicle(session, start_node, end_node, delivery_type, package_id)

        return package_id


if __name__ == "__main__":
    driver = GraphDatabase.driver('bolt://localhost:7687', auth=("neo4j", "bilbo4j"))

    with driver.session() as session:
        # Crear indices y restricciones
        session.run('CREATE CONSTRAINT ON (p:warehouse) ASSERT p.name IS UNIQUE')
        session.run('CREATE CONSTRAINT ON (p:distribution_platform) ASSERT p.name IS UNIQUE')
        session.run('CREATE CONSTRAINT ON (p:delivery_point) ASSERT p.name IS UNIQUE')
        session.run('CREATE CONSTRAINT ON (p:vehicle) ASSERT p.id IS UNIQUE')
        session.run('CREATE CONSTRAINT ON (p:package) ASSERT p.id IS UNIQUE')
        session.run('CREATE CONSTRAINT ON (p:supplier) ASSERT p.id IS UNIQUE')

    id_package = create_delivery(driver, 'Caceres', 'Palencia', 3, 2)

