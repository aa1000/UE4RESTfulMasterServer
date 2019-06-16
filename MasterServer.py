from flask import Flask, Blueprint, request
from flask_restplus import Resource, Api, Namespace, fields, reqparse
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_sqlalchemy import SQLAlchemy
import sqlalchemy_utils
from contextlib import contextmanager
from flask_marshmallow import Marshmallow
from apscheduler.schedulers.background import BackgroundScheduler
import os
import arrow
import atexit


# Init flask app
app = Flask(__name__)
# A fix for the Flask reverse proxy problem
app.wsgi_app = ProxyFix(app.wsgi_app)
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

# "servers" resource RESTful API endpoint definitions
servers_api = Namespace('servers')
api.add_namespace(servers_api)

# Context manager for handling session transactions
@contextmanager
def dbsession():
    """Provide a transactional scope around a series of operations."""
    session = db.session
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

# Game server database table definition
class Server(db.Model):
    url = db.Column(db.String, primary_key=True)
    name = db.Column(db.String())
    game_id = db.Column(db.Integer)
    registration_time = db.Column(sqlalchemy_utils.ArrowType, default=arrow.now())
    ip = db.Column(sqlalchemy_utils.IPAddressType)
    port = db.Column(db.Integer)
    game_mode = db.Column(db.String())
    game_map = db.Column(db.String())
    current_players = db.Column(db.Integer, default=0)
    max_players = db.Column(db.Integer)
    active = db.Column(db.Boolean, default=True)

    def args2query(query_args):
        # Get the values from args and construct a query based on them
        game_id = query_args['game_id']
        game_mode = query_args['game_mode']
        game_map = query_args['game_map']
        max_players = query_args['max_players']
        active = query_args['active']
        slots = query_args['slots']

        query = Server.query

        if game_id:
            query = query.filter(Server.game_id == game_id)
        
        if game_mode:
            query = query.filter(Server.game_mode == game_mode)

        if game_map:
            query = query.filter(Server.game_map == game_map)
        
        if max_players:
            query = query.filter(Server.max_players <= max_players)

        if active:
            query = query.filter(Server.active == active)
        # slots = current number of players - max number of players
        if slots:
            query = query.filter( Server.current_players <= (Server.max_players - slots) )

        return query


# Serialization/Deserialization schema definition
class ServerSchema(ma.ModelSchema):
    strict = True
    class Meta:
        model = Server
        fields = ('url', 'game_id', 'name', 'game_mode', 'game_map',
                    'port', 'current_players', 'max_players')    


# Server model for the interactive flask restplus documentation
# You can remove this if you don't care about the auto-generated docs
api_server_model = api.model('Server',
                    {
                        'name' : fields.String(
                                    description='The name of the server instance',
                                    example='Server Name'),

                        'game_id' : fields.Integer(
                                    description='The (unique) ID of the game running on the server instance',
                                    example=0),

                        'ip' : fields.String(
                                    description='The ip of the machine running the server instance',
                                    example='192.168.1.5'),

                        'port' : fields.Integer(
                                    description='The port the server instance is running on',
                                    example=7777),

                        'game_mode' : fields.String(
                                    description='The name of the game mode running on the current server map',
                                    example='Battle Royal'),

                        'game_map' : fields.String(
                                    description='The name of map running on the server instance',
                                    example='ThirdPersonExampleMap'),

                        'current_players' : fields.Integer(
                                    description='The number of players currently connected to the server',
                                    example=0),

                        'max_players' : fields.Integer(
                                    description='The max number of players who can connect to this server',
                                    example=10)
                    }
                )


# Parser for handling get requests
# This is mostly for the docs as well,
# you can ignore this and handle requests manually since reqparse will get deprecated anyway
server_request_parser = reqparse.RequestParser()

server_request_parser.add_argument(
                        'game_id',
                        type=int,
                        required=False,
                        help='The unique id of the game')

server_request_parser.add_argument(
                        'game_mode',
                        type=str,
                        required=False,
                        help='The map game mode')

server_request_parser.add_argument(
                        'game_map',
                        type=str,
                        required=False,
                        help='The current game map')

server_request_parser.add_argument(
                        'max_players',
                        type=int,
                        required=False, 
                        help='The max number of players allowed on the server')

server_request_parser.add_argument(
                        'active',
                        type=bool,
                        required=False, 
                        help='Whether to only get active (recently checked in) servers or inactive ones')

server_request_parser.add_argument(
                        'slots',
                        type=int,
                        required=False,
                        help='The number of empty player positions on the server')

server_request_parser.add_argument(
                        'limit',
                        type=int,
                        required=False,
                        help='Limit the number of results to this number')

def get_model_dict(model):
    return dict((column.name, getattr(model, column.name)) 
                for column in model.__table__.columns)


@servers_api.route('/')
class ServersList(Resource):

    @servers_api.response(200, 'A list of all servers', [api_server_model])
    @servers_api.response(404, 'Found no servers')
    @servers_api.expect(server_request_parser, validate=True)
    def get(self):
        """
        Query game servers

        <h3>Get a list of all servers that match a certain query</h3>
        """
        query_args = server_request_parser.parse_args()
        # Only the model specific args are processed in the model
        query = Server.args2query(query_args)
        
        limit = query_args['limit']

        if limit and limit > 0:
            query = query.limit(limit)
        
        # Execute the query
        servers = query.all()
        data = ServerSchema(many=True).dump(servers).data
        if servers:
            return data, 200
        else:
            return {'message' : 'No servers found'}, 404

    @servers_api.response(201, 'Server registered in server list')
    @servers_api.response(200, 'Server already present, server info updated')
    @servers_api.response(400, 'Bad Request')
    @servers_api.expect(api_server_model, validate=True)
    def post(self):
        """
        Register game servers

        <h3>register a game server into the database</h3>
        """
        # Create the url form the server ip and the dedicated server port
        # The client address if the address of the http client if one is not provided
        client_addr = request.remote_addr
        if 'ip' in api.payload:
            client_addr = api.payload['ip']
        else:
            api.payload['ip'] = request.remote_addr
        #client_addr = api.payload['ip'] if 'ip' in api.payload and api.payload['ip'] else request.remote_addr
        api.payload['url'] = '{}:{}'.format(client_addr, api.payload['port'])

        
        # validate and deserialize the data
        new_server = ServerSchema().load(api.payload)

        new_server_row = Server.query.get(new_server.data.url)
        # If the server already exists, update all its info and set it to active
        # A server is defined only by its url so the game mode or map could change at any time
        if new_server_row:
            with dbsession():
                new_server_row.active = True
                new_server_row.registration_time = arrow.now()
                Server.query.filter_by(url=new_server.data.url).update(get_model_dict(new_server.data))
            return {'message' : 'Server info updated'}, 200
        # If this is the first time the server is registering with us,
        # then create a new entry for it in the database
        else:
            with dbsession():
                db.session.add(new_server.data)
            return {'message' : 'Server Registered'}, 201


@servers_api.route('/latest')
class ServerLatest(Resource):
    
    @servers_api.response(200, 'The latest checked-in active server matching query')
    @servers_api.response(404, 'No active servers matching query found')
    @servers_api.expect(server_request_parser, validate=True)
    def get(self):
        """
        Request an active game server

        <h3>Get a list of the last checked-in (active) servers that matching a query</h3>
        """
        query_args = server_request_parser.parse_args()
        # Force only active servers when getting the latest server
        query_args['active'] = True
        query = Server.args2query(query_args)

        # Get the latest registered active server
        server = query.order_by(Server.registration_time.desc()).first_or_404()
        return ServerSchema().jsonify(server)

@servers_api.route('/<string:server_url>')
class ServerByURL(Resource):

    @servers_api.response(200, 'The server info', api_server_model)
    @servers_api.response(404, 'Found no servers with this URL')
    def get(self, server_url):
        """
        Request the info of a server

        <h3>Get the info of the server matching the URL</h3>
        """
        server = Server.query.get_or_404(server_url)
        return ServerSchema().jsonify(server)

    # server check in
    @servers_api.response(200, 'Server info updated')
    @servers_api.response(404, 'Failed to update server info')
    @servers_api.expect(api_server_model, validate=True)
    def put(self, server_url):
        """
        Update the info of a server

        <h3>Update the info of the server matching the url</h3>
        """
        # validate and deserialize the data
        new_server_info = ServerSchema().load(api.payload)


        server_row = Server.query.get(server_url)
        # If the server already exists, update all its info and set it to active
        # A server is defined only by its url so the game mode or map could change at any time
        if new_Server_row:
            with dbsession():
                new_server_row.active = True
                new_server_row.registration_time = arrow.now()
                Server.query.filter_by(url=server_url).update(get_model_dict(new_server.data))
            return {'message' : 'Server info updated'}, 200
        # If the server is not registered before then return an 404
        else:
            return {'error' : "Server doesn't exist"}, 404
        
        try:
            with dbsession():
                db.session.add(Server(url=server_url))
            return {'result' : 'Success'}, 200
        except:
            return {'result' : 'Success'}, 404


# TODO: Move to a config file
# 3 seconds seems about right, anything less causes tasks to get called while the previous ones were running
server_inactive_time = 3.0
# sets the server that haven't checked in a while inactive
def set_server_inactive():
    # Query for all the servers that haven't checked-in in more than 'server_inactive_time'
    # Then update all of them to be inactive, only activated by resgtering or checking-in again
    last_active_time = arrow.now().shift(seconds=-server_inactive_time)


    with dbsession():
        Server.query.\
            filter(Server.registration_time < last_active_time, Server.active == True).\
            update(dict(active=False))


# TODO: Move this to seperate class
# Background task to deactivate the servers which missed their check-in.
# Not having the timezone specified gives an error on some docker images and hosting services.
scheduler = BackgroundScheduler({'apscheduler.timezone': 'UTC'})
scheduler.add_job(set_server_inactive, 'interval', seconds=server_inactive_time)
scheduler.start()
# Shutdown the scheduler when this process exits.
atexit.register(lambda: scheduler.shutdown(wait=False))



# User API

# "servers" resource RESTful API endpoint definitions
users_api = Namespace('users')
api.add_namespace(users_api)


# Game server database table definition
class User(db.Model):
    name = db.Column(db.String)
    username = db.Column(db.String)
    email = db.Column(sqlalchemy_utils.EmailType)
    user_id = db.Column(db.Integer, primary_key=True)
    password = db.Column(db.String)

    def args2query(query_args):
        # Get the values from args and construct a query based on them
        name = query_args['name']
        username = query_args['username']
        email = query_args['email']
        user_id = query_args['user_id']
        password = query_args['password']

        query = User.query

        if name:
            query = query.filter(User.name == name)
        
        if username:
            query = query.filter(User.username == username)

        if email:
            query = query.filter(User.email == email)
        
        if user_id:
            query = query.filter(User.user_id == user_id)

        if password:
            query = query.filter(User.password == password)

        return query


# Serialization/Deserialization schema definition
class UserSchema(ma.ModelSchema):
    strict = True
    class Meta:
        model = User 



# You can remove this if you don't care about the auto-generated docs
api_user_model = api.model('User',
                    {
                        'name' : fields.String(
                                    description='The real name of the user',
                                    example='John Doe'),

                        'username' : fields.String(
                                    description='The name of the user account login',
                                    example="JohnDoe2"),

                        'email' : fields.String(
                                    description='The email of the user',
                                    example='user@example.com'),

                        'user_id' : fields.Integer(
                                    description='The id of the user in the database',
                                    example=1),

                        'password' : fields.String(
                                    description='The user login password',
                                    example='Pa$$w0rd_123')
                    }
                )



@users_api.route('/')
class UsersList(Resource):

    @users_api.response(200, 'A list of all users', [api_user_model])
    @users_api.response(404, 'Found no users')
    #@users_api.expect(server_request_parser, validate=True)
    def get(self):
        """
        Query users

        <h3>Get a list of all users that match a certain query</h3>
        """
        #query_args = user_request_parser.parse_args()
        # Only the model specific args are processed in the model
        #query = User.args2query(query_args)
        
        # Execute the query
        #users = query.all()
        users = User.query.all()
        data = UserSchema(many=True).dump(users).data
        if users:
            return data, 200
        else:
            return {'message' : 'No users found'}, 404

    @users_api.response(201, 'User registered successfully')
    @users_api.response(200, 'User already present, user info updated')
    @users_api.response(400, 'Bad Request')
    @users_api.expect(api_user_model, validate=True)
    def post(self):
        """
        Register user

        <h3>register a user into the database</h3>
        """
        # validate and deserialize the data
        new_user = UserSchema().load(users_api.payload)

        new_user_row = User.query.get(new_user.data.user_id)
        # If the user already exists, update all their info
        # A user is defined only by their user_id so the rest could change at any time
        if new_user_row:
            with dbsession():
                User.query.filter_by(user_id=new_user.data.user_id).update(get_model_dict(new_user.data))
            return {'message' : 'User info updated'}, 200
        # If this is the first time the user is registering with us,
        # then create a new entry for them in the database
        else:
            with dbsession():
                db.session.add(new_user.data)
            return {'message' : 'User Registered'}, 201


@users_api.route('/<int:user_id>')
class ServerByID(Resource):

    @users_api.response(200, 'The user info', api_user_model)
    @users_api.response(404, 'Found no user with this user_id')
    def get(self, user_id):
        """
        Request the info of a user

        <h3>Get the info of the user matching the user_id</h3>
        """
        user = User.query.get_or_404(user_id)
        return UserSchema().jsonify(user)


# Run server
if __name__ == '__main__':

    # Create the database if it doesn't exist
    if not os.path.isfile(database_path):
        db.create_all()

    app.debug = True
    app.run(host='0.0.0.0')
    
