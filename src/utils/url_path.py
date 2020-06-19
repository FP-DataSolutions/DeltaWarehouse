class UrlPath:
    @staticmethod
    def combine(*args):
        result = ''
        for path in args:
            result += path if path.endswith('/') else '{}/'.format(path)
        #result = result[:-1]
        return result