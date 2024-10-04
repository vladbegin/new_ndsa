from .command import router as command_router
from .callback import router as callback_router


# Collect all router instances in the list
routers = [
    command_router,
    callback_router
]