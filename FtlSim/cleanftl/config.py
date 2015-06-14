import json

class Config(dict):
    def show(self):
        print self

    def load_from_dict(self, dic):
        super(Config, self).clear()
        super(Config, self).__init__(dic)

    def load_from_json_file(self, file_path):
        decoded = json.load(open(file_path, 'r'))
        self.load_from_dict(decoded)

# a = Config()
# a['2'] = 3
# a.show()
# a.load_from_dict({4:88})
# a.show()
# a.load_from_json_file('./config.json')
# a.show()

