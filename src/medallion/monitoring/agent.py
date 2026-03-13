"""Monitoring Agent - Collects metrics and performs health checks"""

import logging
import time
from typing import Dict, Any, Optional, Callable, List
from datetime import datetime
import psycopg2

from .models import MetricPoint, HealthCheckResult, HealthStatus

logger = logging.getLogger(__name__)


class MonitoringAgent:
    """Collects metrics and performs health checks for medallion components"""

    def __init__(self, db_connection_string: str):
        """Initialize Monitoring Agent"""
        self.db_connection_string = db_connection_string
        self.metrics: List[MetricPoint] = []
        self.health_checks: Dict[str, HealthCheckResult] = {}
        self.metrics_buffer_size = 1000

    def _get_connection(self):
        """Get database connection"""
        return psycopg2.connect(self.db_connection_string)

    def record_metric(
        self,
        metric_name: str,
        metric_value: float,
        metric_unit: str = "",
        labels: Optional[Dict[str, str]] = None
    ) -> MetricPoint:
        """Record a metric"""
        metric = MetricPoint(
            metric_name=metric_name,
            metric_value=metric_value,
            metric_unit=metric_unit,
            labels=labels or {}
        )
        
        self.metrics.append(metric)
        
        # Flush metrics if buffer is full
        if len(self.metrics) >= self.metrics_buffer_size:
            self._flush_metrics()
        
        logger.debug(f"Recorded metric {metric_name}: {metric_value} {metric_unit}")
        return metric

    def _flush_metrics(self) -> None:
        """Flush metrics to database"""
        if not self.metrics:
            return
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            for metric in self.metrics:
                cursor.execute("""
                    INSERT INTO monitoring.pipeline_metrics
                    (layer, metric_name, metric_value, metric_unit)
                    VALUES (%s, %s, %s, %s)
                """, (
                    metric.labels.get("layer", "unknown"),
                    metric.metric_name,
                    metric.metric_value,
                    metric.metric_unit
                ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Flushed {len(self.metrics)} metrics to database")
            self.metrics.clear()
            
        except Exception as e:
            logger.error(f"Error flushing metrics: {e}")

    def check_health(
        self,
        component_name: str,
        check_func: Callable[[], bool],
        timeout_ms: int = 5000
    ) -> HealthCheckResult:
        """
        Perform a health check on a component
        
        Args:
            component_name: Name of the component
            check_func: Function that returns True if healthy
            timeout_ms: Timeout in milliseconds
            
        Returns:
            HealthCheckResult
        """
        start_time = time.time()
        
        try:
            # Execute health check with timeout
            is_healthy = check_func()
            response_time_ms = int((time.time() - start_time) * 1000)
            
            result = HealthCheckResult(
                component_name=component_name,
                status=HealthStatus.HEALTHY if is_healthy else HealthStatus.UNHEALTHY,
                message="Component is healthy" if is_healthy else "Component is unhealthy",
                response_time_ms=response_time_ms
            )
            
        except Exception as e:
            response_time_ms = int((time.time() - start_time) * 1000)
            result = HealthCheckResult(
                component_name=component_name,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check failed: {str(e)}",
                response_time_ms=response_time_ms
            )
        
        self.health_checks[component_name] = result
        logger.debug(f"Health check {component_name}: {result.status.value}")
        return result

    def get_health_status(self) -> Dict[str, HealthStatus]:
        """Get overall health status of all components"""
        return {
            name: result.status
            for name, result in self.health_checks.items()
        }

    def get_overall_health(self) -> HealthStatus:
        """Get overall health status"""
        statuses = list(self.get_health_status().values())
        
        if not statuses:
            return HealthStatus.HEALTHY
        
        if any(s == HealthStatus.UNHEALTHY for s in statuses):
            return HealthStatus.UNHEALTHY
        elif any(s == HealthStatus.DEGRADED for s in statuses):
            return HealthStatus.DEGRADED
        else:
            return HealthStatus.HEALTHY

    def record_alert(
        self,
        alert_name: str,
        severity: str,
        message: str,
        affected_component: str
    ) -> bool:
        """Record an alert"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO monitoring.pipeline_alerts
                (alert_name, severity, message, affected_component)
                VALUES (%s, %s, %s, %s)
            """, (alert_name, severity, message, affected_component))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.warning(f"Alert recorded: {alert_name} - {message}")
            return True
            
        except Exception as e:
            logger.error(f"Error recording alert: {e}")
            return False

    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of recorded metrics"""
        if not self.metrics:
            return {"total_metrics": 0}
        
        metric_names = {}
        for metric in self.metrics:
            if metric.metric_name not in metric_names:
                metric_names[metric.metric_name] = 0
            metric_names[metric.metric_name] += 1
        
        return {
            "total_metrics": len(self.metrics),
            "metric_types": metric_names,
            "buffer_size": len(self.metrics)
        }
