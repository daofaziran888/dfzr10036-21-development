"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, sink_check

# 专用前缀路由必须排在通用 /api/{module} 资源路由之前，否则固定路径
# （如 /api/sink-check/verify）会被 /api/{module}/{entry_id} 抢先匹配。
ROUTERS = [sink_check, resource]
