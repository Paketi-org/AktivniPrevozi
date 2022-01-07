import requests
import googlemaps
from flask import Flask, request
from flask_restx import Resource, Api, fields, reqparse, abort, marshal, marshal_with
from configparser import ConfigParser
import psycopg2 as pg
from psycopg2 import extensions
from healthcheck import HealthCheck, EnvironmentDump
from prometheus_flask_exporter import PrometheusMetrics, RESTfulPrometheusMetrics
from fluent import sender, handler
import logging
from time import time
import json
import os
import socket
from datetime import datetime
import consul

app = Flask(__name__)

# Load configurations from the config file
def load_configurations():
    app.config.from_file("config.json", load=json.load)

    with open("config.json") as json_file:
        data = json.load(json_file)
        # Override variables defined in the environment over the ones from the config file
        for item in data:
            if os.environ.get(item):
                app.config[item] = os.environ.get(item)


load_configurations()

c = consul.Consul(host=app.config["CONSUL_IP"], port=app.config["CONSUL_PORT"])


@app.route("/")
def welcome():
    return "Welcome!", 200


@app.route("/aktivni_prevozi/konfiguracija")
def konfiguracija():
    _, data = c.kv.get("aktivni_prevozi/test")
    return data["Value"]


custom_format = {
    "name": "%(name_of_service)s",
    "method": "%(crud_method)s",
    "traffic": "%(directions)s",
    "ip": "%(ip_node)s",
    "status": "%(status)s",
    "code": "%(http_code)s",
}
logging.basicConfig(level=logging.INFO)
l = logging.getLogger("Aktivni prevozi")
h = handler.FluentHandler(
    "Aktivni prevozi", host=app.config["FLUENT_IP"], port=int(app.config["FLUENT_PORT"])
)
formatter = handler.FluentRecordFormatter(custom_format)
h.setFormatter(formatter)
l.addHandler(h)

l.info(
    "Setting up Aktivni prevozi App",
    extra={
        "name_of_service": "Aktivni prevozi",
        "crud_method": None,
        "directions": None,
        "ip_node": None,
        "status": None,
        "http_code": None,
    },
)

api = Api(
    app,
    version="1.0",
    doc="/aktivni_prevozi/openapi",
    title="Aktivni prevozi API",
    description="Abstrakt Aktivni prevozi API",
    default_swagger_filename="openapi.json",
    default="Aktivni prevozi CRUD",
    default_label="koncne tocke in operacije",
)
prevozApiModel = api.model(
    "ModelPrevoza",
    {
        "id_prevoza": fields.Integer(readonly=True, description="ID prevoza"),
        "prevoznik": fields.Integer(readonly=True, description="ID prevoznika"),
        "uporabnik_prevoza": fields.Integer(
            readonly=True, description="ID uporabnika prevoza"
        ),
        "cas_prihoda": fields.String(readonly=True, description="Cas odhoda"),
        "cas_odhoda": fields.String(readonly=True, description="Cas prihoda"),
        "od_lokacije": fields.String(readonly=True, description="Lokacija pobiranja"),
        "do_lokacije": fields.String(readonly=True, description="Lokacija dostave"),
        "status": fields.String(readonly=True, description="Status prevoza"),
        "trenutna_lokacija": fields.String(
            readonly=True, description="Trenutna lokacija"
        ),
        "odpremljeno": fields.String(readonly=True, description="Odpremljeno"),
        "prejeto": fields.String(readonly=True, description="Prejeto"),
        "strosek": fields.Integer(readonly=True, description="Strosek prevoza"),
    },
)
prevoziApiModel = api.model(
    "ModelPrevozov", {"prevozi": fields.List(fields.Nested(prevozApiModel))}
)
ns = api.namespace(
    "Aktivni prevozi CRUD", description="Aktivni prevozi koncne tocke in operacije"
)
posodobiModel = api.model(
    "PosodobiPrevoz", {"atribut": fields.String, "vrednost": fields.String}
)

gmaps = googlemaps.Client(key="AIzaSyCvKiN13Ed9dtbjsNgP6TXgjl7udKB5Ass")


def check_database_connection():
    conn = connect_to_database()
    if conn.poll() == extensions.POLL_OK:
        print("POLL: POLL_OK")
    if conn.poll() == extensions.POLL_READ:
        print("POLL: POLL_READ")
    if conn.poll() == extensions.POLL_WRITE:
        print("POLL: POLL_WRITE")
    _, data = c.kv.get("aktivni_prevozi/zdravje")
    if data["Value"] == "NOT OK":
        return False
    return True, "Database connection OK"


def application_data():
    return {"maintainer": "Matevž Morato", "git_repo": "https://github.com/Paketi-org/"}


def connect_to_database():
    return pg.connect(
        database=app.config["PGDATABASE"],
        user=app.config["PGUSER"],
        password=app.config["PGPASSWORD"],
        port=app.config["DATABASE_PORT"],
        host=app.config["DATABASE_IP"],
    )


prevoziPolja = {
    "id_prevoza": fields.Integer,
    "prevoznik": fields.Integer,
    "uporabnik_prevoza": fields.Integer,
    "od_lokacije": fields.String,
    "do_lokacije": fields.String,
    "cas_odhoda": fields.String,
    "cas_prihoda": fields.String,
    "trenutna_lokacija": fields.String,
    "odpremljeno": fields.String,
    "prejeto": fields.String,
    "status": fields.String,
    "strosek": fields.Integer,
}


class PrevozModel:
    def __init__(
        self,
        id_prevoza,
        prevoznik,
        uporabnik_prevoza,
        od_lokacije,
        do_lokacije,
        cas_odhoda,
        cas_prihoda,
        trenutna_lokacija,
        odpremljeno,
        prejeto,
        status,
        strosek,
    ):
        self.id_prevoza = id_prevoza
        self.prevoznik = prevoznik
        self.uporabnik_prevoza = uporabnik_prevoza
        self.od_lokacije = od_lokacije
        self.do_lokacije = do_lokacije
        self.cas_odhoda = cas_odhoda
        self.cas_prihoda = cas_prihoda
        self.trenutna_lokacija = trenutna_lokacija
        self.odpremljeno = odpremljeno
        self.prejeto = prejeto
        self.status = status
        self.strosek = strosek


class Prevoz(Resource):
    def __init__(self, *args, **kwargs):
        self.table_name = "aktivni_prevozi"
        self.placila_storitev = app.config["PLACILA_IP"]
        self.uporabniki = app.config["UPORABNIKI_IP"]
        self.ponujeni = app.config["VOZNIKI_IP"]
        self.iskani = app.config["ISKALCI_IP"]
        self.conn = connect_to_database()
        self.cur = self.conn.cursor()
        self.cur.execute(
            "select exists(select * from information_schema.tables where table_name=%s)",
            (self.table_name,),
        )
        if self.cur.fetchone()[0]:
            print("Table {0} already exists".format(self.table_name))
        else:
            self.cur.execute(
                """CREATE TABLE aktivni_prevozi (
                           id_prevoza INT NOT NULL,
                           prevoznik INT NOT NULL,
                           uporabnik_prevoza INT NOT NULL,
                           od_lokacije CHAR(20),
                           do_lokacije CHAR(20),
                           cas_odhoda CHAR(20),
                           cas_prihoda CHAR(20),
                           trenutna_lokacija CHAR(20),
                           odpremljeno CHAR(20),
                           prejeto CHAR(20),
                           status CHAR(20),
                           strosek INT
                        )"""
            )

        self.parser = reqparse.RequestParser()
        self.parser.add_argument(
            "id_prevoza", type=int, help="Unikaten ID ponujenega prevoza"
        )
        self.parser.add_argument("prevoznik", type=int, help="Unikaten ID prevoznika")
        self.parser.add_argument(
            "uporabnik_prevoza", type=int, help="Unikaten ID uporabnika prevoza"
        )
        self.parser.add_argument("od_lokacije", type=str, help="lokacije pobiranja")
        self.parser.add_argument("do_lokacije", type=str, help="lokacije dostave")
        self.parser.add_argument("cas_odhoda", type=str, help="cas pobiranja")
        self.parser.add_argument(
            "trenutna_lokacija", type=str, help="trenutna lokacija"
        )
        self.parser.add_argument("odpremljeno", type=str, help="odpremljeno")
        self.parser.add_argument("prejeto", type=str, help="prejeto")
        self.parser.add_argument("status", type=str, help="status dostave")
        self.parser.add_argument("strosek", type=str, help="strosek dostave")
        self.parser.add_argument("atribut", type=str)
        self.parser.add_argument("vrednost")

        super(Prevoz, self).__init__(*args, **kwargs)

    @marshal_with(prevozApiModel)
    @ns.response(404, "Prevoz ni najden")
    @ns.doc("Vrni prevoz")
    def get(self, id):
        """
        Vrni podatke prevoza glede na ID
        """
        l.info(
            "Zahtevaj prevoz z ID %s" % str(id),
            extra={
                "name_of_service": "Aktivni prevozi",
                "crud_method": "get",
                "directions": "in",
                "ip_node": socket.gethostbyname(socket.gethostname()),
                "status": None,
                "http_code": None,
            },
        )

        self.cur.execute(
            "SELECT * FROM aktivni_prevozi WHERE id_prevoza = %s" % str(id)
        )
        row = self.cur.fetchall()

        if len(row) == 0:
            l.warning(
                "Prevoz z ID %s ni bil najden in ne bo izbrisan" % str(id),
                extra={
                    "name_of_service": "Aktivni prevozi",
                    "crud_method": "get",
                    "directions": "out",
                    "ip_node": socket.gethostbyname(socket.gethostname()),
                    "status": "fail",
                    "http_code": 404,
                },
            )
            abort(404)

        d = {}
        for el, k in zip(row[0], prevoziPolja):
            d[k] = el

        prevoz = PrevozModel(
            id_prevoza=d["id_prevoza"],
            prevoznik=d["prevoznik"],
            uporabnik_prevoza=d["uporabnik_prevoza"],
            od_lokacije=d["od_lokacije"].strip(),
            do_lokacije=d["do_lokacije"].strip(),
            cas_odhoda=d["cas_odhoda"].strip(),
            cas_prihoda=d["cas_prihoda"].strip(),
            trenutna_lokacija=d["trenutna_lokacija"].strip(),
            odpremljeno=d["odpremljeno"].strip(),
            prejeto=d["prejeto"].strip(),
            status=d["status"].strip(),
            strosek=d["strosek"],
        )

        l.info(
            "Vrni prevoz z ID %s" % str(id),
            extra={
                "name_of_service": "Aktivni prevozi",
                "crud_method": "get",
                "directions": "out",
                "ip_node": socket.gethostbyname(socket.gethostname()),
                "status": "success",
                "http_code": 200,
            },
        )

        return prevoz, 200

    @marshal_with(prevozApiModel)
    @ns.expect(posodobiModel)
    @ns.response(404, "Prevoz ni najden")
    @ns.doc("Posodobi prevoz")
    def put(self, id):
        """
        Posodobi podatke prevoza glede na ID
        """
        l.info(
            "Posodobi prevoz z ID %s" % str(id),
            extra={
                "name_of_service": "Aktivni prevozi",
                "crud_method": "put",
                "directions": "in",
                "ip_node": socket.gethostbyname(socket.gethostname()),
                "status": None,
                "http_code": None,
            },
        )

        args = self.parser.parse_args()
        attribute = args["atribut"]  # uporabnik_prevoza
        value = args["vrednost"]  # id

        self.cur.execute(
            """UPDATE {0} SET {1} = '{2}' WHERE id_prevoza = {3}""".format(
                self.table_name, attribute, value, id
            )
        )
        self.conn.commit()

        self.cur.execute(
            "SELECT * FROM aktivni_prevozi WHERE id_prevoza = %s" % str(id)
        )
        row = self.cur.fetchall()

        if len(row) == 0:
            l.warning(
                "Prevoz z ID %s ni bil najden" % str(id),
                extra={
                    "name_of_service": "Aktivni prevozi",
                    "crud_method": "put",
                    "directions": "out",
                    "ip_node": socket.gethostbyname(socket.gethostname()),
                    "status": "fail",
                    "http_code": 404,
                },
            )
            abort(404)

        d = {}
        for el, k in zip(row[0], prevoziPolja):
            d[k] = el
        prevoz = PrevozModel(
            id_prevoza=d["id_prevoza"],
            prevoznik=d["prevoznik"],
            uporabnik_prevoza=d["uporabnik_prevoza"],
            od_lokacije=d["od_lokacije"].strip(),
            do_lokacije=d["do_lokacije"].strip(),
            cas_odhoda=d["cas_odhoda"].strip(),
            cas_prihoda=d["cas_prihoda"].strip(),
            trenutna_lokacija=d["trenutna_lokacija"].strip(),
            odpremljeno=d["odpremljeno"].strip(),
            prejeto=d["prejeto"].strip(),
            status=d["status"].strip(),
            strosek=d["strosek"],
        )

        # Posodobi transakcijo
        l.info(
            "Posodobi transakcijo",
            extra={
                "name_of_service": "Aktivni prevozi",
                "crud_method": "put",
                "directions": "out",
                "ip_node": socket.gethostbyname(socket.gethostname()),
                "status": None,
                "http_code": None,
            },
        )
        return prevoz, 200

    @ns.doc("Izbrisi prevoz")
    @ns.response(404, "Prevoz ni najden")
    @ns.response(204, "Prevoz izbrisan")
    def delete(self, id):
        """
        Izbriši prevoz glede na ID
        """
        l.info(
            "Izbrisi prevoz z ID %s" % str(id),
            extra={
                "name_of_service": "Aktivni prevozi",
                "crud_method": "delete",
                "directions": "in",
                "ip_node": socket.gethostbyname(socket.gethostname()),
                "status": None,
                "http_code": None,
            },
        )
        self.cur.execute("SELECT * FROM aktivni_prevozi")
        rows = self.cur.fetchall()
        ids = []
        for row in rows:
            ids.append(row[0])

        if id not in ids:
            l.warning(
                "Prevoz z ID %s ni bil najden in ne bo izbrisan" % str(id),
                extra={
                    "name_of_service": "Aktivni prevozi",
                    "crud_method": "delete",
                    "directions": "out",
                    "ip_node": socket.gethostbyname(socket.gethostname()),
                    "status": "fail",
                    "http_code": 404,
                },
            )
            abort(404)
        else:
            self.cur.execute(
                "DELETE FROM aktivni_prevozi WHERE id_prevoza = %s" % str(id)
            )
            self.conn.commit()

        l.info(
            "Prevoz z ID %s izbrisan" % str(id),
            extra={
                "name_of_service": "Aktivni prevozi",
                "crud_method": "delete",
                "directions": "out",
                "ip_node": socket.gethostbyname(socket.gethostname()),
                "status": "success",
                "http_code": 204,
            },
        )

        return 204


class ListPrevozov(Resource):
    def __init__(self, *args, **kwargs):
        self.table_name = "aktivni_prevozi"
        self.placila_storitev = app.config["PLACILA_IP"]
        self.uporabniki = app.config["UPORABNIKI_IP"]
        self.ponujeni = app.config["VOZNIKI_IP"]
        self.iskani = app.config["ISKALCI_IP"]
        self.conn = connect_to_database()
        self.cur = self.conn.cursor()
        self.cur.execute(
            "select exists(select * from information_schema.tables where table_name=%s)",
            (self.table_name,),
        )
        if self.cur.fetchone()[0]:
            print("Table {0} already exists".format(self.table_name))
        else:
            self.cur.execute(
                """CREATE TABLE aktivni_prevozi (
                           id_prevoza INT NOT NULL,
                           prevoznik INT NOT NULL,
                           uporabnik_prevoza INT NOT NULL,
                           od_lokacije CHAR(20),
                           do_lokacije CHAR(20),
                           cas_odhoda CHAR(20),
                           cas_prihoda CHAR(20),
                           trenutna_lokacija CHAR(20),
                           odpremljeno CHAR(20),
                           prejeto CHAR(20),
                           status CHAR(20),
                           strosek INT
                        )"""
            )
        self.parser = reqparse.RequestParser()
        self.parser.add_argument(
            "id_prevoza",
            type=int,
            required=True,
            help="Unikaten ID ponujenega prevoza mora biti nastavljen",
        )
        self.parser.add_argument(
            "vir", type=str, required=True, help="Vir mora biti nastavljen"
        )
        self.parser.add_argument(
            "uporabnik_prevoza",
            type=int,
            required=True,
            help="Unikaten ID uporabnika prevoza mora biti določen, če še neznan nastavi na -1",
        )

        super(ListPrevozov, self).__init__(*args, **kwargs)

    @ns.marshal_list_with(prevoziApiModel)
    @ns.doc("Vrni vse prevoze")
    def get(self):
        """
        Vrni vse prevoze
        """
        l.info(
            "Zahtevaj vse prevoze",
            extra={
                "name_of_service": "Aktivni prevozi",
                "crud_method": "get",
                "directions": "in",
                "ip_node": socket.gethostbyname(socket.gethostname()),
                "status": None,
                "http_code": None,
            },
        )

        self.cur.execute("SELECT * FROM aktivni_prevozi")
        rows = self.cur.fetchall()
        ds = {}
        i = 0
        for row in rows:
            ds[i] = {}
            for el, k in zip(row, prevoziPolja):
                ds[i][k] = el
            i += 1

        prevozi = []
        for d in ds:
            prevoz = PrevozModel(
                id_prevoza=ds[d]["id_prevoza"],
                prevoznik=ds[d]["prevoznik"],
                uporabnik_prevoza=ds[d]["uporabnik_prevoza"],
                od_lokacije=ds[d]["od_lokacije"].strip(),
                do_lokacije=ds[d]["do_lokacije"].strip(),
                cas_odhoda=ds[d]["cas_odhoda"].strip(),
                cas_prihoda=ds[d]["cas_prihoda"].strip(),
                trenutna_lokacija=ds[d]["trenutna_lokacija"].strip(),
                odpremljeno=ds[d]["odpremljeno"].strip(),
                prejeto=ds[d]["prejeto"].strip(),
                status=ds[d]["status"].strip(),
                strosek=ds[d]["strosek"],
            )
            prevozi.append(prevoz)

        l.info(
            "Vrni vse prevoze",
            extra={
                "name_of_service": "Aktivni prevozi",
                "crud_method": "get",
                "directions": "out",
                "ip_node": socket.gethostbyname(socket.gethostname()),
                "status": "success",
                "http_code": 200,
            },
        )

        return {"prevozi": prevozi}, 200

    @marshal_with(prevozApiModel)
    @ns.expect(prevozApiModel)
    @ns.doc("Dodaj prevoz")
    def post(self):
        """
        Dodaj nov prevoz
        """
        l.info(
            "Dodaj nov prevoz",
            extra={
                "name_of_service": "Aktivni prevozi",
                "crud_method": "post",
                "directions": "in",
                "ip_node": socket.gethostbyname(socket.gethostname()),
                "status": None,
                "http_code": None,
            },
        )

        args = self.parser.parse_args()

        id = args["uporabnik_prevoza"]
        # Preveri če prevoznik obstaja
        resp = requests.get(self.uporabniki + "/narocniki/%s" % str(id))
        if resp.status_code != 200:
            l.warning("Uporabnik z ID %s ni bil najden" % str(id), extra={"name_of_service": "Aktivni prevozi", "crud_method": "post", "directions": "out", "ip_node": socket.gethostbyname(socket.gethostname()), "status": "fail", "http_code": 404})
            abort(410, "Ta uporabnik ne obstaja!")

        values = []
        for a in args.values():
            values.append(a)

        # # Dodaj čas prihoda from Google API
        # t = datetime.strptime('18/1/22 '+str(args["cas_odhoda"]), '%d/%m/%y %H:%M')
        # directions = gmaps.directions(args["od_lokacije"], args["do_lokacije"], mode="driving", departure_time=t)
        # time_arrival = directions[0]['legs'][0]['duration_in_traffic']['text']
        # values.insert(6, time_arrival)

        id = None
        vir = None
        ## args["uporabnik_prevoza"]
        ## args["id"]
        ## args["vir"]

        if args["vir"] == "iskani":
            vir = self.iskani + "iskani_prevozi/" + str(args["id_prevoza"])
        elif args["vir"] == "ponujeni":
            vir = self.ponujeni + "ponujeni_prevozi/" + str(args["id_prevoza"])
        else:
            l.warning('Zahteva mora biti ali "iskani" ali "ponujeni"')
            abort(408)

        resp = requests.get(vir)
        if resp.status_code != 200:
            l.warning(
                "Prevoz z ID %s ni bil najden" % str(args["id_prevoza"]),
                extra={
                    "name_of_service": "Aktivni prevozi",
                    "crud_method": "post",
                    "directions": "out",
                    "ip_node": socket.gethostbyname(socket.gethostname()),
                    "status": "fail",
                    "http_code": 404,
                },
            )
            abort(404)
        pd = resp.json()
        requests.delete(vir)  # Zbrisi prevoz iz tam kjer je prisel

        self.cur.execute(
            """INSERT INTO {0} (id_prevoza, prevoznik, uporabnik_prevoza, od_lokacije, do_lokacije, cas_odhoda, cas_prihoda, trenutna_lokacija, odpremljeno, prejeto, status, strosek)
                VALUES ({1}, {2}, {3}, '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}')""".format(
                "aktivni_prevozi",
                pd["id_prevoza"],
                pd["prevoznik"],
                args["uporabnik_prevoza"],
                pd["od_lokacije"],
                pd["do_lokacije"],
                pd["cas_odhoda"],
                pd["cas_prihoda"],
                pd["od_lokacije"],
                "Ne",
                "Ne",
                "V teku",
                pd["strosek"],
            )
        )
        self.conn.commit()
        prevoz = PrevozModel(
            id_prevoza=pd["id_prevoza"],
            uporabnik_prevoza=pd["uporabnik_prevoza"],
            prevoznik=pd["prevoznik"],
            od_lokacije=pd["od_lokacije"],
            do_lokacije=pd["do_lokacije"],
            cas_odhoda=pd["cas_odhoda"],
            cas_prihoda=pd["cas_prihoda"],
            trenutna_lokacija=pd["od_lokacije"],
            odpremljeno="Ne",
            prejeto="Ne",
            status=pd["status"],
            strosek=pd["strosek"],
        )

        # Ustvari transakcijo
        l.info(
            "Nov prevoz dodan",
            extra={
                "name_of_service": "Aktivni prevozi",
                "crud_method": "post",
                "directions": "out",
                "ip_node": socket.gethostbyname(socket.gethostname()),
                "status": "success",
                "http_code": 201,
            },
        )

        return prevoz, 201


health = HealthCheck()
envdump = EnvironmentDump()
health.add_check(check_database_connection)
envdump.add_section("application", application_data)
app.add_url_rule("/healthcheck", "healthcheck", view_func=lambda: health.run())
app.add_url_rule("/environment", "environment", view_func=lambda: envdump.run())
api.add_resource(ListPrevozov, "/aktivni_prevozi")
api.add_resource(Prevoz, "/aktivni_prevozi/<int:id>")
app.run(host="0.0.0.0", port=5011)
h.close()
