import asyncio
import traceback

class App():
    """
    App class combines a number of agents and runs them in a asyncio task.
    This class serves as an entry point to run the agents
    """

    def __init__(self, agents):
        """

        Args:
            agents (agent): agents that the app class will run
        """
        self.agents = agents


    async def run(self):
        """
        Run the agents
        """
        tasks = []
        for agent in self.agents:
            tasks.append(asyncio.create_task(agent.run()))
        await asyncio.gather(*tasks)



class App_Factory():
    """
    Factory class for Agent class
    """

    def __init__(self, factory_register):
        """

        Args:
            factory_register (dict): key is the name of the config store section,
                value is a reference to the corresponding agent factory class
        """
        self.factory_register = factory_register

    def create_from_config(self, config_store):
        """
        Creates and returns an App from a config store

        Args:
            config_store (config store): config store with the configuration of the apps

        Returns:
            App: generated App class from the config store
        """
        agents = []

        for section, factory in self.factory_register.items():
            try:
                agent = factory.create_from_config(config_store, section)
                if agent is not None:
                    agents += agent
            except Exception:
                traceback.print_exc()
        
        return App(agents)

