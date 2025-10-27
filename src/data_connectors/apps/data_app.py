from mainsequence.virtualfundbuilder.portfolio_interface import PortfolioInterface

from mainsequence.virtualfundbuilder.models import PortfolioConfiguration
from mainsequence.virtualfundbuilder.utils import get_vfb_logger

from pydantic import BaseModel
from mainsequence.virtualfundbuilder.resource_factory.app_factory import BaseApp, register_app

logger = get_vfb_logger()


class ExampleAppConfiguration(BaseModel):
    age: float=27
    name: str="Daniel Garber"

@register_app()
class ExampleApp(BaseApp):
    configuration_class = ExampleAppConfiguration

    def __init__(self, configuration: ExampleAppConfiguration):
        self.configuration = configuration

    def run(self) -> None:

        logger.info(f"This tool is been used by {self.configuration.name} whom is {self.configuration.age}")
        # self.add_output(output=portfolio.target_portfolio)


if __name__ == "__main__":
    configuration = ExampleAppConfiguration()
    ExampleApp(configuration).run()