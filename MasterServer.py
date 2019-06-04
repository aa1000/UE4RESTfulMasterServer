from flask import Flask, Blueprint, request
from flask_restplus import Resource, Api, Namespace, fields
from flask_sqlalchemy import SQLAlchemy
import sqlalchemy_utils
from flask_marshmallow import Marshmallow
from apscheduler.schedulers.background import BackgroundScheduler
import os
import arrow
import atexit


# Init app
app = Flask(__name__)
# Add a blueprint to move the api end point
blueprint = Blueprint('api', __name__, url_prefix='/api')
# move the documentation end point as well
api = Api(blueprint, doc='/docs')
# register the blueprint in the app
app.register_blueprint(blueprint)

# Get the path for the root (current) directory
basedir = os.path.abspath(os.path.dirname(__file__))
# Get the path for the database
database_path = os.path.join(basedir, 'Database/GameServers.db')

# Database
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + database_path
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Init SQLAlchemy DB
db = SQLAlchemy(app)
# Init Marshmallow
ma = Marshmallow(app)

# Game server database table definition
class Server(db.Model):
    url = db.Column(db.String, primary_key=True)
    name = db.Column(db.String())
    game_id = db.Column(db.Integer)
    registration_time = db.Column(sqlalchemy_utils.ArrowType, default=arrow.now())
    ip = db.Column(sqlalchemy_utils.IPAddressType)
    port = db.Column(db.Integer)
    game_mode = db.Column(db.String())
    map = db.Column(db.String())
    current_players = db.Column(db.Integer, default=0)
    max_players = db.Column(db.Integer)
    active = db.Column(db.Boolean, default=True)


# Serialization/Deserialization schema definition
class ServerSchema(ma.Schema):
    class Meta:
        
        fields = ('url', 'game_id', 'name', 'game_mode', 'map',
                    'port', 'current_players', 'max_players')    
        

# "servers" resource RESTful API definitions
servers_api = Namespace('servers')
api.add_namespace(servers_api)

# Init Schemas
server_schema = ServerSchema(strict=True)
servers_schema = ServerSchema(many=True, strict=True)

api_server_model = api.model('Server',
                    {
                        'game_id' : fields.Integer('Game id.'),
                        'name' : fields.String('Server name.'),
                        'port' : fields.Integer('Server port.'),
                    })



@servers_api.route('/')
class ServersList(Resource):

    @servers_api.response(200, 'Get a list of all servers')
    def get(self):
        servers = Server.query.all()
        return servers_schema.jsonify(servers)

    @servers_api.response(201, 'Server already present, updated the server info.')
    @servers_api.response(201, 'Added server to server list')
    @servers_api.response(400, 'Bad Request')
    #@api.expect(api_server_model)
    def post(self):
        # Create the url form the server ip and the dedicated server port
        api.payload['url'] = '{}:{}'.format(request.remote_addr, api.payload['port'])
        # validate the data
        new_server = server_schema.load(api.payload)


        new_Server_row = Server.query.get(new_server.data['url'])
        # if the server already exists, update all its info and set it to active
        # a server is only defined by its url so the game mode or map could change at any time
        if new_Server_row:
            new_Server_row.active = True
            new_Server_row.registration_time = arrow.now()
            Server.query.filter_by(url=new_server.data['url']).update(new_server.data)
            db.session.commit()
            return {'result' : 'Success'}, 200
        # if this is the first time the server is registering with us,
        # then create a new entry for it in the database
        else:
            try:
                db.session.add(Server(**new_server.data))
                db.session.commit()
                return {'result' : 'Success'}, 201
            except:
                db.session.rollback()
                return {'result' : 'Fail'}, 400

@servers_api.route('/latest')
class ServerLatest(Resource):

    @servers_api.response(200, 'The latest registered active server')
    @servers_api.response(404, 'No active server found')
    def get(self):
        # Get the latest registered active server
        server = Server.query.filter_by(active=True).order_by(Server.registration_time.desc()).first_or_404()
        return server_schema.jsonify(server)

@servers_api.route('/<string:server_url>')
class ServerByID(Resource):

    #@servers_api.response(ServerSchema())
    def get(self, server_url):
        server = Server.query.get_or_404(server_url)
        return server_schema.jsonify(server)

    # server check in
    @servers_api.response(200, 'Server info updated')
    @servers_api.response(404, 'Failed to update server info')
    def put(self, server_url):
        print(api.request.remote_addr)

        try:
            db.session.add(Server(url=server_url))
            db.session.commit()
            return 200
        except:
            return 404



# TODO: Move to a config file
server_inactive_time = 1.0
# sets the server that haven't checked in a while inactive
def set_server_inactive():
    # Query for all the servers that haven't checked-in in more than 'server_inactive_time'
    # Then update all of them to be inactive, only activated by resgtering or checking-in again
    last_active_time = arrow.now().shift(seconds=-server_inactive_time)
    Server.query.\
       filter(Server.registration_time < last_active_time).\
       update(dict(active=False))
    db.session.commit()

# TODO: Move this to seperate class
# Background task to deactivate the servers which missed their check-in
scheduler = BackgroundScheduler()
scheduler.add_job(set_server_inactive, 'interval', seconds=server_inactive_time)
scheduler.start()
# Shutdown the scheduler when this process exits
atexit.register(lambda: scheduler.shutdown(wait=False))

# Run server
if __name__ == '__main__':

    # Create the database if it doesn't exist
    if not os.path.isfile(database_path):
        db.create_all()

    app.run(debug=True)
    