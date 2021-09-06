# encoding: utf-8
"""
阿尔法研究平台

Project: sustecher
Author: Moses
E-mail: 8342537@qq.com
"""
import os
import ast
import base64
import pyarrow
from pyarrow.flight import (ServerAuthHandler, ServerMiddleware, ServerMiddlewareFactory)

import jaqs.util as jutil


def case_insensitive_header_lookup(headers, lookup_key):
    """Lookup the value of given key in the given headers.
       The key lookup is case insensitive.
    """
    for key in headers:
        if key.lower() == lookup_key.lower():
            return headers.get(key)


class NoopAuthHandler(ServerAuthHandler):
    """A no-op auth handler."""

    def authenticate(self, outgoing, incoming):
        """Do nothing."""

    def is_valid(self, token):
        """
        Returning an empty string.
        Returning None causes Type error.
        """
        return ""


class HeaderAuthServerMiddleware(ServerMiddleware):
    """A ServerMiddleware that transports incoming username and passowrd."""

    def __init__(self, token):
        self.token = token

    def sending_headers(self):
        return {'authorization': 'Bearer ' + self.token}


class HeaderAuthServerMiddlewareFactory(ServerMiddlewareFactory):
    """Validates incoming username and password."""

    def start_call(self, info, headers):
        print(headers)
        auth_header = case_insensitive_header_lookup(headers, 'Authorization')
        values = auth_header[0].split(' ')
        token = ''
        error_message = 'Invalid credentials'
        if values[0] == 'Basic':
            decoded = base64.b64decode(values[1])
            pair = decoded.decode("utf-8").split(':')
            print(pair)
            passport = pair[0]
            password = pair[1]
            #检验账号密码是否输入正确
            if not passport.startswith("researcher") or password != "123456":
                raise pyarrow.flight.FlightUnauthenticatedError(error_message)
            token = passport
        elif values[0] == 'Bearer':
            token = values[1]
        else:
            raise pyarrow.flight.FlightUnauthenticatedError(error_message)

        return HeaderAuthServerMiddleware(token)


class FlightServer(pyarrow.flight.FlightServerBase):

    def __init__(self, host="localhost", location=None, auth_handler=None, tls_certificates=None, verify_client=False, root_certificates=None, middleware=None):
        super(FlightServer, self).__init__(location, auth_handler, tls_certificates, verify_client, root_certificates, middleware)
        self.flights = {}
        self.host = host

    @classmethod
    def descriptor_to_key(self, descriptor):
        return (descriptor.descriptor_type.value, descriptor.command, tuple(descriptor.path or tuple()))

    def _make_flight_info(self, key, descriptor, table):
        location = pyarrow.flight.Location.for_grpc_tcp(self.host, self.port)
        endpoints = [pyarrow.flight.FlightEndpoint(repr(key), [location]), ]

        mock_sink = pyarrow.MockOutputStream()
        stream_writer = pyarrow.RecordBatchStreamWriter(mock_sink, table.schema)
        stream_writer.write_table(table)
        stream_writer.close()
        data_size = mock_sink.size()

        return pyarrow.flight.FlightInfo(table.schema, descriptor, endpoints, table.num_rows, data_size)

    def list_flights(self, context, criteria):
        for key, table in self.flights.items():
            if key[1] is not None:
                descriptor = pyarrow.flight.FlightDescriptor.for_command(key[1])
            else:
                descriptor = pyarrow.flight.FlightDescriptor.for_path(*key[2])

            yield self._make_flight_info(key, descriptor, table)

    def get_flight_info(self, context, descriptor):
        key = FlightServer.descriptor_to_key(descriptor)
        if key in self.flights:
            table = self.flights[key]
            return self._make_flight_info(key, descriptor, table)
        raise KeyError('Flight not found.')

    def do_put(self, context, descriptor, reader, writer):
        middleware = context.get_middleware("auth")
        if not middleware:
            raise pyarrow.flight.FlightUnauthenticatedError('No token auth middleware found.')
        auth_header = case_insensitive_header_lookup(middleware.sending_headers(), 'Authorization')
        values = auth_header.split(' ')
        dirname = values[1]
        #import pdb;pdb.set_trace()
        key = FlightServer.descriptor_to_key(descriptor)
        print(key)
        self.flights[key] = reader.read_all()
        #print(self.flights[key])
        #新添加的部分
        pathdir = os.path.abspath("datahouse")
        df = self.flights[key].to_pandas()
        fp = os.path.join(pathdir, dirname, descriptor.path[0].decode(encoding='utf-8')+'.csv')
        print(fp)
        jutil.create_dir(fp)
        df.to_csv(fp, encoding='gbk')
        print(df.head())

    def do_get(self, context, ticket):
        key = ast.literal_eval(ticket.ticket.decode())
        if key not in self.flights:
            return None
        return pyarrow.flight.RecordBatchStream(self.flights[key])

    def list_actions(self, context):
        return [
            ("clear", "Clear the stored flights."),
            ("healthcheck", "Check this server."),
        ]

    def do_action(self, context, action):
        if action.type == "clear":
            raise NotImplementedError("{} is not implemented.".format(action.type))
        elif action.type == "healthcheck":
            return ["healthcheck".encode("utf-8")]
        else:
            raise KeyError("Unknown action {!r}".format(action.type))


def main():
    location = "grpc+tcp://localhost:5005"
    server = FlightServer("localhost", location, NoopAuthHandler(), None, False, None, {"auth": HeaderAuthServerMiddlewareFactory()})
    print("Serving on", location)
    server.serve()


if __name__ == '__main__':
    main()
