import requests
import clint.textui

tests_get = [
    ("/users/1", lambda d: d.json()["first_name"], "Пётр"),
    ("/users/1000000", lambda d: d.status_code, 404),
    ("/users/44/visits", lambda d: len(d.json()["visits"]), 31),
    ("/users/1000000/visits", lambda d: d.status_code, 404),
    ("/locations/5", lambda d: d.json()["city"], "Санктгород"),
    ("/locations/1000000", lambda d: d.status_code, 404),
    ("/locations/115/avg", lambda d: d.json()["avg"], 2.5),
    ("/locations/1000000/avg", lambda d: d.status_code, 404),
    ("/visits/5", lambda d: d.json()["user"], 53),
    ("/visits/1000000", lambda d: d.status_code, 404),
]

clint.textui.puts(clint.textui.colored.blue("========================= GET =============================="))

for (url, handler, truth) in tests_get:
    data = requests.get("http://localhost:8080" + url)
    result = handler(data)
    if result == truth:
        clint.textui.puts(clint.textui.colored.green("GET  %s: %s == %s" % (url, result, truth)))
    else:
        clint.textui.puts(clint.textui.colored.red("GET  %s: %s != %s" % (url, result, truth)))

clint.textui.puts(clint.textui.colored.blue("========================= POST =============================="))

user_data = """{"email": "johndoe1@gmail.com","first_name": "Jessie","last_name": "Pinkman","birth_date": 616550400, "gender": "m"}"""
user_data_bad_email = """{"email": "johndoe1","first_name": "Jessie","last_name": "Pinkman","birth_date": 616550400, "gender": "m"}"""
user_data_rus = """{"email": "johndoe2@gmail.com","first_name": "Маша","last_name": "Иванова","birth_date": 616550400, "gender": "m"}"""
user_data_new = """{"id": 808081, "email": "johndoe3@gmail.com","first_name": "Jessie","last_name": "Pinkman","birth_date": 616550400, "gender": "m"}"""

loc_data = """{"distance":61,"city":"San Andreas","place":"House","country":"Indonesia"}"""
loc_data_new = """{"id": 808081, "distance":61,"city":"San Andreas","place":"House","country":"Indonesia"}"""
loc_data_null = """{"city":null,"place":"House"}"""

visit_data = """{"user":53,"location":7,"visited_at":1279680878,"mark":1}"""
visit_data_new = """{"id":808081,"user":53,"location":7,"visited_at":1279680878,"mark":1}"""

tests_post = [
    ("/users/51", user_data, lambda d: d.json() == {}, "/users/51", lambda d: d.json()["first_name"] == "Jessie"),
    ("/users/8080", user_data, lambda d: d.status_code == 404, "/users/8080", lambda d: d.status_code == 404),
    ("/users/new", user_data_new, lambda d: d.json() == {}, "/users/808081", lambda d: d.json()["first_name"] == "Jessie"),

    ("/locations/51", loc_data, lambda d: d.json() == {}, "/locations/51", lambda d: d.json()["city"] == "San Andreas"),
    ("/locations/8080", loc_data, lambda d: d.status_code == 404, "/locations/8080", lambda d: d.status_code == 404),
    ("/locations/new", loc_data_new, lambda d: d.json() == {}, "/locations/808081", lambda d: d.json()["city"] == "San Andreas"),

    ("/visits/51", visit_data, lambda d: d.json() == {}, "/visits/51", lambda d: d.json()["visited_at"] == 1279680878),
    ("/visits/808080", visit_data, lambda d: d.status_code == 404, "/visits/808080", lambda d: d.status_code == 404),
    ("/visits/new", visit_data_new, lambda d: d.json() == {}, "/visits/808081", lambda d: d.json()["visited_at"] == 1279680878),

    ("/users/49", user_data_bad_email, lambda d: d.status_code == 400, "/users/49", lambda d: d.json()["email"] == "heblinarinihpenhih@inbox.ru"),
    ("/users/52", user_data_rus, lambda d: d.json() == {}, "/users/52", lambda d: d.json()["first_name"] == "Маша"),
    ("/users/53", user_data_new, lambda d: d.status_code == 400, "/users/53", lambda d: d.json()["email"] == "sawihmod@mail.ru"),
    ("/locations/53", loc_data_new, lambda d: d.status_code == 400, "/locations/53", lambda d: d.json()["city"] == "Лесоатск"),
    ("/locations/54", loc_data_null, lambda d: d.status_code == 400, "/locations/54", lambda d: d.json()["city"] == "Лейпштадт"),
    ("/visits/53", visit_data_new, lambda d: d.status_code == 400, "/visits/53", lambda d: d.json()["visited_at"] == 1277194880),
]

for (url_post, post, handler_post, url_get, handler_get) in tests_post:
    data = requests.post("http://localhost:8080" + url_post, post.encode('utf-8'))
    if handler_post(data):
        clint.textui.puts(clint.textui.colored.green("POST %s ok" % url_post))
    else:
        clint.textui.puts(clint.textui.colored.red("POST %s NOT ok" % url_post))

    data = requests.get("http://localhost:8080" + url_get)
    if handler_get(data):
        clint.textui.puts(clint.textui.colored.green("GET  %s" % url_get))
    else:
        print(data.status_code)
        print(data.content)
        clint.textui.puts(clint.textui.colored.red("GET  %s" % url_get))

clint.textui.puts(clint.textui.colored.blue("========================= GET params =============================="))

tests_get_params = [
    ("/users/44/visits?fromDate=1117282582", lambda d: len(d.json()["visits"]), 16),
    ("/users/44/visits?toDate=1117282582", lambda d: len(d.json()["visits"]), 14),
    ("/users/44/visits?country=Россия", lambda d: len(d.json()["visits"]), 2),
    ("/users/44/visits?toDistance=5", lambda d: len(d.json()["visits"]), 2),
    ("/locations/115/avg?fromDate=1117282582", lambda d: d.json()["avg"], 2.64286),
    ("/locations/115/avg?toDate=1117282582", lambda d: d.json()["avg"], 2.16667),
    ("/locations/115/avg?fromAge=10", lambda d: d.json()["avg"], 2.47368),
    ("/locations/115/avg?toAge=10", lambda d: d.json()["avg"], 3.0),
    ("/locations/115/avg?gender=m", lambda d: d.json()["avg"], 2.54545),

    ("/users/44/visits?fromDate=BJX4QRXsUQujBfJGwJOT3tN0wP6GEeAI", lambda d: d.status_code, 400),
    ("/users/44/visits?toDate=F3ZYFexm8ppiLuvGJ9DJbfHOU9q127BK", lambda d: d.status_code, 400),
    ("/users/44/visits?toDistance=ecbbbaebabeeaecdadedbbedcdbcddea", lambda d: d.status_code, 400),
    ("/locations/115/avg?fromDate=ecbbbaebabeeaecdadedbbedcdbcddea", lambda d: d.status_code, 400),
    ("/locations/115/avg?toDate=ecbbbaebabeeaecdadedbbedcdbcddea", lambda d: d.status_code, 400),
    ("/locations/115/avg?fromAge=ecbbbaebabeeaecdadedbbedcdbcddea", lambda d: d.status_code, 400),
    ("/locations/115/avg?toAge=ecbbbaebabeeaecdadedbbedcdbcddea", lambda d: d.status_code, 400),
    ("/locations/115/avg?gender=ecbbbaebabeeaecdadedbbedcdbcddea", lambda d: d.status_code, 400),
]

for (url, handler, truth) in tests_get_params:
    data = requests.get("http://localhost:8080" + url)
    result = handler(data)
    if result == truth:
        clint.textui.puts(clint.textui.colored.green("GET  %s: %s == %s" % (url, result, truth)))
    else:
        clint.textui.puts(clint.textui.colored.red("GET  %s: %s != %s" % (url, result, truth)))

clint.textui.puts(clint.textui.colored.blue("========================= Visit POSTs =============================="))

data = requests.get("http://localhost:8080/users/46/visits").json()['visits']
clint.textui.puts(clint.textui.colored.green("BEFORE user visits %s %s" % (len(data), sum([v['mark'] for v in data]))))

data = requests.get("http://localhost:8080/locations/64/avg").json()['avg']
clint.textui.puts(clint.textui.colored.green("BEFORE loc avg %s" % data))

data = requests.post("http://localhost:8080/visits/123", """{"mark": 1}""")
clint.textui.puts(clint.textui.colored.green("UPDATE mark"))

data = requests.get("http://localhost:8080/users/46/visits").json()['visits']
clint.textui.puts(clint.textui.colored.green("MARK user visits %s %s" % (len(data), sum([v['mark'] for v in data]))))

data = requests.get("http://localhost:8080/locations/64/avg").json()['avg']
clint.textui.puts(clint.textui.colored.green("MARK loc avg %s" % data))

data = requests.post("http://localhost:8080/visits/123", """{"location": 111}""")
clint.textui.puts(clint.textui.colored.green("UPDATE location"))

data = requests.get("http://localhost:8080/users/46/visits").json()['visits']
clint.textui.puts(clint.textui.colored.green("LOCATION user visits %s %s" % (len(data), sum([v['mark'] for v in data]))))

data = requests.get("http://localhost:8080/locations/64/avg").json()['avg']
clint.textui.puts(clint.textui.colored.green("LOCATION loc avg %s" % data))

data = requests.get("http://localhost:8080/users/111/visits").json()['visits']
clint.textui.puts(clint.textui.colored.green("USER 111 visits %s %s" % (len(data), sum([v['mark'] for v in data]))))

data = requests.post("http://localhost:8080/visits/123", """{"user": 111}""")
clint.textui.puts(clint.textui.colored.green("UPDATE user"))

data = requests.get("http://localhost:8080/users/46/visits").json()['visits']
clint.textui.puts(clint.textui.colored.green("USER user visits %s %s" % (len(data), sum([v['mark'] for v in data]))))

data = requests.get("http://localhost:8080/users/111/visits").json()['visits']
clint.textui.puts(clint.textui.colored.green("USER 111 visits %s %s" % (len(data), sum([v['mark'] for v in data]))))

data = requests.get("http://localhost:8080/locations/64/avg").json()['avg']
clint.textui.puts(clint.textui.colored.green("USER loc avg %s" % data))


