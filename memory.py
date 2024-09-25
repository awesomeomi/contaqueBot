class BotMemory:
    def __init__(self):
        self.memory = {
            "last_resource": None,
            "last_condition": None,
            "last_query_type": None,
        }

    def update_memory(self, resource, conditions, query_type):
        self.memory["last_resource"] = resource
        self.memory["last_condition"] = conditions
        self.memory["last_quer_type"] = query_type

        print(self.memory["last_resource"])
    
    def get_memory(self):
        return self.memory
    
    def clear_memory(self):
        self.memory= {
            "last_resource": None,
            "last_condition": None,
            "last_query_type": None,
        }