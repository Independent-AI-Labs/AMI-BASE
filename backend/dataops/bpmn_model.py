"""
BPMN 2.0-compatible data model extending the security model
Implements a minimalistic but comprehensive BPMN structure
"""
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import Field

from ..utils.uuid_utils import uuid7
from .security_model import SecuredStorageModel
from .storage_types import StorageConfig, StorageType


# BPMN Event Types
class EventType(str, Enum):
    """BPMN 2.0 Event Types"""

    START = "start"
    END = "end"
    INTERMEDIATE = "intermediate"
    BOUNDARY = "boundary"


class EventDefinition(str, Enum):
    """BPMN 2.0 Event Definitions"""

    NONE = "none"
    MESSAGE = "message"
    TIMER = "timer"
    ERROR = "error"
    ESCALATION = "escalation"
    CANCEL = "cancel"
    COMPENSATION = "compensation"
    CONDITIONAL = "conditional"
    LINK = "link"
    SIGNAL = "signal"
    TERMINATE = "terminate"
    MULTIPLE = "multiple"
    PARALLEL_MULTIPLE = "parallelMultiple"


# BPMN Gateway Types
class GatewayType(str, Enum):
    """BPMN 2.0 Gateway Types"""

    EXCLUSIVE = "exclusive"
    INCLUSIVE = "inclusive"
    PARALLEL = "parallel"
    COMPLEX = "complex"
    EVENT_BASED = "eventBased"
    PARALLEL_EVENT_BASED = "parallelEventBased"


# BPMN Task Types
class TaskType(str, Enum):
    """BPMN 2.0 Task Types"""

    NONE = "none"
    USER = "user"
    MANUAL = "manual"
    SERVICE = "service"
    SCRIPT = "script"
    BUSINESS_RULE = "businessRule"
    SEND = "send"
    RECEIVE = "receive"
    CALL_ACTIVITY = "callActivity"


# BPMN Process Types
class ProcessType(str, Enum):
    """BPMN 2.0 Process Types"""

    NONE = "none"
    PUBLIC = "public"
    PRIVATE = "private"
    COLLABORATION = "collaboration"


# Task/Flow Node States
class TaskStatus(str, Enum):
    """Task execution states"""

    READY = "ready"
    PENDING = "pending"  # Alias for ready
    ACTIVE = "active"
    IN_PROGRESS = "in_progress"  # Alias for active
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SUSPENDED = "suspended"
    COMPENSATING = "compensating"


# Base BPMN Classes
class BPMNElement(SecuredStorageModel):
    """Base class for all BPMN elements"""

    # BPMN Core attributes
    element_id: str = Field(default_factory=lambda: f"bpmn_{uuid7()}")
    name: str | None = None
    documentation: str | None = None

    # Extension attributes
    attributes: dict[str, Any] = Field(default_factory=dict)

    class Meta:
        storage_configs = {
            "graph": StorageConfig(storage_type=StorageType.GRAPH),
            "document": StorageConfig(storage_type=StorageType.DOCUMENT),
        }
        indexes = [{"field": "element_id", "type": "hash"}, {"field": "name", "type": "text"}]


class FlowNode(BPMNElement):
    """Base class for all flow nodes (Activities, Events, Gateways)"""

    # Connections
    incoming_flows: list[str] = Field(default_factory=list)  # IDs of incoming sequence flows
    outgoing_flows: list[str] = Field(default_factory=list)  # IDs of outgoing sequence flows

    # Lane assignment
    lane_id: str | None = None

    # Execution state
    state: TaskStatus = TaskStatus.READY

    # Metrics
    start_time: datetime | None = None
    end_time: datetime | None = None
    duration_ms: int | None = None


class SequenceFlow(BPMNElement):
    """Sequence flow connecting flow nodes"""

    source_ref: str  # ID of source flow node
    target_ref: str  # ID of target flow node

    # Conditional flow
    condition_expression: str | None = None
    is_default: bool = False

    # Execution
    tokens_passed: int = 0
    last_token_time: datetime | None = None

    class Meta:
        storage_configs = {
            "graph": StorageConfig(storage_type=StorageType.GRAPH),
            "cache": StorageConfig(storage_type=StorageType.CACHE),
        }
        indexes = [{"field": "source_ref", "type": "hash"}, {"field": "target_ref", "type": "hash"}]


# Events
class Event(FlowNode):
    """BPMN Event"""

    event_type: EventType
    event_definition: EventDefinition = EventDefinition.NONE

    # Event-specific data
    event_data: dict[str, Any] = Field(default_factory=dict)

    # For catching events
    is_interrupting: bool = True

    # For timer events
    timer_definition: dict[str, Any] | None = None  # cycle, date, duration

    # For message events
    message_ref: str | None = None

    # For error events
    error_code: str | None = None

    # For signal events
    signal_ref: str | None = None


# Activities
class Task(FlowNode):
    """BPMN Task/Activity"""

    task_type: TaskType = TaskType.NONE

    # Task implementation
    implementation: str | None = None  # Service endpoint, script, etc.

    # Loop characteristics
    is_loop: bool = False
    is_sequential: bool = True
    loop_condition: str | None = None
    loop_maximum: int | None = None

    # Multi-instance
    is_multi_instance: bool = False
    multi_instance_collection: str | None = None
    completion_condition: str | None = None

    # Compensation
    is_for_compensation: bool = False
    compensation_handler: str | None = None

    # Resources
    performer_ref: str | None = None  # Role/User who performs the task
    assignee: str | None = None  # Specific user assigned to the task
    resources: list[str] = Field(default_factory=list)

    # Dependencies
    dependencies: list[str] = Field(default_factory=list)  # IDs of tasks that must complete first

    # Input/Output
    input_sets: list[dict[str, Any]] = Field(default_factory=list)
    output_sets: list[dict[str, Any]] = Field(default_factory=list)

    # Execution metrics
    retry_count: int = 0
    max_retries: int = 3
    timeout_seconds: int | None = None

    # Task timing (in addition to FlowNode's start_time/end_time)
    started_at: datetime | None = None
    completed_at: datetime | None = None


class SubProcess(Task):
    """BPMN SubProcess - contains other flow nodes"""

    # Contained elements
    flow_nodes: list[str] = Field(default_factory=list)  # IDs of contained nodes
    sequence_flows: list[str] = Field(default_factory=list)  # IDs of internal flows

    # SubProcess types
    is_event_subprocess: bool = False
    triggered_by_event: bool = False

    # Transaction
    is_transaction: bool = False
    transaction_method: str | None = None


# Gateways
class Gateway(FlowNode):
    """BPMN Gateway"""

    gateway_type: GatewayType

    # For event-based gateways
    instantiate: bool = False
    event_gateway_type: str | None = None  # exclusive, parallel

    # Default flow
    default_flow: str | None = None

    # Gateway direction
    gateway_direction: str = "unspecified"  # converging, diverging, mixed, unspecified


# Collaboration Elements
class Pool(BPMNElement):
    """BPMN Pool - represents a participant"""

    participant_ref: str | None = None  # Reference to participant/organization
    process_ref: str | None = None  # Reference to contained process

    # Pool properties
    is_executable: bool = True
    is_closed: bool = False

    # Contained lanes
    lanes: list[str] = Field(default_factory=list)  # IDs of lanes

    class Meta:
        storage_configs = {
            "graph": StorageConfig(storage_type=StorageType.GRAPH),
            "document": StorageConfig(storage_type=StorageType.DOCUMENT),
        }
        path = "pools"


class Lane(BPMNElement):
    """BPMN Lane - subdivision of a pool"""

    pool_id: str  # Parent pool ID

    # Lane properties
    parent_lane_id: str | None = None  # For nested lanes
    child_lanes: list[str] = Field(default_factory=list)

    # Contained flow nodes
    flow_node_refs: list[str] = Field(default_factory=list)

    # Performer
    performer_ref: str | None = None  # Role/User assigned to this lane


class MessageFlow(BPMNElement):
    """Message flow between pools"""

    source_ref: str  # Source pool/activity ID
    target_ref: str  # Target pool/activity ID

    # Message
    message_ref: str | None = None

    # Execution
    messages_sent: int = 0
    last_message_time: datetime | None = None


# Data Elements
class DataObject(BPMNElement):
    """BPMN Data Object"""

    # Data properties
    data_state: str | None = None
    is_collection: bool = False

    # Data value
    data_value: Any = None
    data_type: str | None = None

    # References
    item_subject_ref: str | None = None


class DataStore(DataObject):
    """BPMN Data Store - persistent data"""

    capacity: int | None = None
    is_unlimited: bool = True

    # Storage backend
    storage_type: str = "document"
    storage_config: dict[str, Any] = Field(default_factory=dict)


# Resources
class Resource(BPMNElement):
    """BPMN Resource"""

    resource_type: str  # human, system, service

    # Resource properties
    availability: float = 1.0  # 0-1 availability factor
    capacity: int = 1  # Number of parallel executions
    cost_per_hour: float | None = None

    # Current allocation
    allocated_to: list[str] = Field(default_factory=list)  # Task IDs
    utilization: float = 0.0  # Current utilization percentage


class Role(Resource):
    """BPMN Role/Performer"""

    role_name: str
    members: list[str] = Field(default_factory=list)  # User IDs

    # Permissions
    allowed_tasks: list[str] = Field(default_factory=list)  # Task types this role can perform

    class Meta:
        storage_configs = {
            "graph": StorageConfig(storage_type=StorageType.GRAPH),
            "cache": StorageConfig(storage_type=StorageType.CACHE),
        }


# Process Definition
class Process(BPMNElement):
    """BPMN Process Definition"""

    process_type: ProcessType = ProcessType.PRIVATE

    # Process properties
    is_executable: bool = True
    is_closed: bool = False

    # Version control
    version: str = "1.0.0"
    is_latest: bool = True
    previous_version: str | None = None

    # Process elements
    flow_nodes: list[str] = Field(default_factory=list)
    sequence_flows: list[str] = Field(default_factory=list)
    data_objects: list[str] = Field(default_factory=list)

    # Participants
    participants: list[str] = Field(default_factory=list)

    # Goals (extension for goal-oriented processes)
    goals: list[str] = Field(default_factory=list)

    # Metrics
    instances_created: int = 0
    instances_completed: int = 0
    average_duration_ms: int | None = None

    class Meta:
        storage_configs = {
            "graph": StorageConfig(storage_type=StorageType.GRAPH),
            "document": StorageConfig(storage_type=StorageType.DOCUMENT),
            "cache": StorageConfig(storage_type=StorageType.CACHE),
        }
        path = "processes"
        indexes = [{"field": "name", "type": "text"}, {"field": "version", "type": "hash"}, {"field": "is_latest", "type": "hash"}]


class ProcessInstance(SecuredStorageModel):
    """Runtime instance of a BPMN process"""

    # Process reference
    process_id: str  # ID of process definition
    process_version: str

    # Instance properties
    instance_id: str = Field(default_factory=lambda: f"instance_{uuid7()}")
    parent_instance_id: str | None = None  # For sub-processes

    # State
    state: str = "created"  # created, active, completed, terminated, suspended

    # Tokens (for execution flow)
    tokens: list[dict[str, Any]] = Field(default_factory=list)

    # Variables (process data)
    variables: dict[str, Any] = Field(default_factory=dict)

    # Current activities
    active_tasks: list[str] = Field(default_factory=list)

    # History
    completed_tasks: list[str] = Field(default_factory=list)

    # Timing
    start_time: datetime = Field(default_factory=datetime.utcnow)
    end_time: datetime | None = None
    duration_ms: int | None = None

    # Correlation
    correlation_key: str | None = None
    business_key: str | None = None

    class Meta:
        storage_configs = {
            "graph": StorageConfig(storage_type=StorageType.GRAPH),
            "timeseries": StorageConfig(storage_type=StorageType.TIMESERIES),  # For metrics
            "cache": StorageConfig(storage_type=StorageType.CACHE),  # For active instances
        }
        path = "process_instances"
        indexes = [
            {"field": "process_id", "type": "hash"},
            {"field": "state", "type": "hash"},
            {"field": "correlation_key", "type": "hash"},
            {"field": "business_key", "type": "hash"},
        ]


# Goals (Extension for goal-oriented BPMN)
class Goal(BPMNElement):
    """Goal in a goal-oriented process"""

    # Goal properties
    goal_type: str = "achievement"  # achievement, maintenance, avoidance
    priority: int = 1  # 1-10

    # Goal conditions
    satisfaction_condition: str  # Expression to evaluate
    failure_condition: str | None = None

    # Related elements
    supporting_tasks: list[str] = Field(default_factory=list)
    conflicting_goals: list[str] = Field(default_factory=list)

    # State
    is_satisfied: bool = False
    is_failed: bool = False
    satisfaction_level: float = 0.0  # 0-1

    # Metrics
    attempts: int = 0
    success_rate: float = 0.0

    class Meta:
        storage_configs = {
            "graph": StorageConfig(storage_type=StorageType.GRAPH),
            "document": StorageConfig(storage_type=StorageType.DOCUMENT),
        }
        path = "goals"


# Collaboration/Choreography
class Collaboration(BPMNElement):
    """BPMN Collaboration - interaction between participants"""

    # Participants
    participants: list[str] = Field(default_factory=list)  # Pool IDs

    # Message flows
    message_flows: list[str] = Field(default_factory=list)

    # Conversations
    conversations: list[str] = Field(default_factory=list)

    # Choreography
    choreography_ref: str | None = None

    class Meta:
        storage_configs = {
            "graph": StorageConfig(storage_type=StorageType.GRAPH),
            "document": StorageConfig(storage_type=StorageType.DOCUMENT),
        }
        path = "collaborations"


class Message(BPMNElement):
    """BPMN Message"""

    # Message properties
    message_type: str  # request, response, event, command

    # Content
    payload: Any = None
    headers: dict[str, str] = Field(default_factory=dict)

    # Correlation
    correlation_key: str | None = None
    conversation_id: str | None = None

    # Routing
    source_participant: str | None = None
    target_participant: str | None = None

    # Timing
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    expiry_time: datetime | None = None

    # State
    is_consumed: bool = False
    consumed_by: str | None = None

    class Meta:
        storage_configs = {
            "cache": StorageConfig(storage_type=StorageType.CACHE),  # For active messages
            "document": StorageConfig(storage_type=StorageType.DOCUMENT),  # For persistence
        }
        path = "messages"


# Monitoring and Metrics
class ProcessMetrics(SecuredStorageModel):
    """Process execution metrics"""

    process_id: str
    time_window: str  # hour, day, week, month

    # Counters
    instances_started: int = 0
    instances_completed: int = 0
    instances_failed: int = 0

    # Timing
    min_duration_ms: int | None = None
    max_duration_ms: int | None = None
    avg_duration_ms: int | None = None

    # Task metrics
    task_metrics: dict[str, dict[str, Any]] = Field(default_factory=dict)

    # Resource utilization
    resource_utilization: dict[str, float] = Field(default_factory=dict)

    # SLA compliance
    sla_met: int = 0
    sla_violated: int = 0

    class Meta:
        storage_configs = {
            "timeseries": StorageConfig(storage_type=StorageType.TIMESERIES),
            "document": StorageConfig(storage_type=StorageType.DOCUMENT),
        }
        path = "process_metrics"


# Helper functions for BPMN model management
def create_process_from_bpmn(bpmn_xml: str) -> Process:
    """Parse BPMN XML and create Process model"""
    # This would parse BPMN 2.0 XML and create model instances
    # Implementation would use xml.etree or lxml


def export_process_to_bpmn(process: Process) -> str:
    """Export Process model to BPMN 2.0 XML"""
    # This would generate valid BPMN 2.0 XML
    # Implementation would build XML structure


def validate_bpmn_model(process: Process) -> tuple[bool, list[str]]:
    """Validate BPMN model for correctness"""
    errors = []

    # Check for start events
    start_events = [node for node in process.flow_nodes if isinstance(node, Event) and node.event_type == EventType.START]
    if not start_events:
        errors.append("Process must have at least one start event")

    # Check for end events
    end_events = [node for node in process.flow_nodes if isinstance(node, Event) and node.event_type == EventType.END]
    if not end_events:
        errors.append("Process must have at least one end event")

    # Check connectivity
    # ... additional validation logic

    return len(errors) == 0, errors
