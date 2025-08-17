"""
Tests for BPMN 2.0 data model
"""
from datetime import datetime

from services.dataops.bpmn_model import (
    BPMNElement,
    Event,
    EventDefinition,
    EventType,
    FlowNode,
    Gateway,
    GatewayType,
    Goal,
    Lane,
    Message,
    Pool,
    Process,
    ProcessInstance,
    ProcessType,
    SequenceFlow,
    SubProcess,
    Task,
    TaskType,
)
from services.utils.uuid_utils import is_uuid7


class TestBPMNEnums:
    """Test BPMN enumeration values"""

    def test_event_types(self):
        """Test event type enum"""
        assert EventType.START.value == "start"
        assert EventType.END.value == "end"
        assert EventType.INTERMEDIATE.value == "intermediate"
        assert EventType.BOUNDARY.value == "boundary"

    def test_event_definitions(self):
        """Test event definition enum"""
        assert EventDefinition.NONE.value == "none"
        assert EventDefinition.MESSAGE.value == "message"
        assert EventDefinition.TIMER.value == "timer"
        assert EventDefinition.ERROR.value == "error"
        assert EventDefinition.SIGNAL.value == "signal"

    def test_gateway_types(self):
        """Test gateway type enum"""
        assert GatewayType.EXCLUSIVE.value == "exclusive"
        assert GatewayType.INCLUSIVE.value == "inclusive"
        assert GatewayType.PARALLEL.value == "parallel"
        assert GatewayType.EVENT_BASED.value == "eventBased"

    def test_task_types(self):
        """Test task type enum"""
        assert TaskType.USER.value == "user"
        assert TaskType.SERVICE.value == "service"
        assert TaskType.SCRIPT.value == "script"
        assert TaskType.MANUAL.value == "manual"

    def test_process_types(self):
        """Test process type enum"""
        assert ProcessType.PRIVATE.value == "private"
        assert ProcessType.PUBLIC.value == "public"
        assert ProcessType.COLLABORATION.value == "collaboration"


class TestBPMNElement:
    """Test base BPMN element"""

    def test_element_creation(self):
        """Test BPMN element creation"""
        element = BPMNElement(name="Test Element")

        assert element.name == "Test Element"
        assert element.element_id.startswith("bpmn_")
        assert is_uuid7(element.element_id.replace("bpmn_", ""))
        assert element.documentation is None
        assert element.attributes == {}

    def test_element_with_attributes(self):
        """Test element with custom attributes"""
        element = BPMNElement(name="Custom Element", documentation="Test documentation", attributes={"custom": "value", "priority": 1})

        assert element.documentation == "Test documentation"
        assert element.attributes["custom"] == "value"
        assert element.attributes["priority"] == 1


class TestFlowNode:
    """Test flow node base class"""

    def test_flow_node_creation(self):
        """Test flow node creation"""
        node = FlowNode(name="Test Node")

        assert node.name == "Test Node"
        assert node.incoming_flows == []
        assert node.outgoing_flows == []
        assert node.lane_id is None
        assert node.state == "ready"
        assert node.start_time is None
        assert node.end_time is None

    def test_flow_node_with_connections(self):
        """Test flow node with connections"""
        node = FlowNode(name="Connected Node", incoming_flows=["flow1", "flow2"], outgoing_flows=["flow3"], lane_id="lane1")

        assert len(node.incoming_flows) == 2
        assert "flow1" in node.incoming_flows
        assert len(node.outgoing_flows) == 1
        assert node.lane_id == "lane1"


class TestEvent:
    """Test BPMN events"""

    def test_start_event(self):
        """Test start event creation"""
        event = Event(name="Start", event_type=EventType.START, event_definition=EventDefinition.NONE)

        assert event.name == "Start"
        assert event.event_type == EventType.START
        assert event.event_definition == EventDefinition.NONE
        assert event.is_interrupting is True

    def test_timer_event(self):
        """Test timer event"""
        event = Event(
            name="Timer",
            event_type=EventType.INTERMEDIATE,
            event_definition=EventDefinition.TIMER,
            timer_definition={"duration": "PT1H"},  # 1 hour
        )

        assert event.event_definition == EventDefinition.TIMER
        assert event.timer_definition["duration"] == "PT1H"

    def test_message_event(self):
        """Test message event"""
        event = Event(name="Message Event", event_type=EventType.INTERMEDIATE, event_definition=EventDefinition.MESSAGE, message_ref="msg_123")

        assert event.event_definition == EventDefinition.MESSAGE
        assert event.message_ref == "msg_123"


class TestTask:
    """Test BPMN tasks"""

    def test_user_task(self):
        """Test user task creation"""
        task = Task(name="User Task", task_type=TaskType.USER, performer_ref="user_role")

        assert task.name == "User Task"
        assert task.task_type == TaskType.USER
        assert task.performer_ref == "user_role"
        assert task.retry_count == 0
        assert task.max_retries == 3

    def test_service_task(self):
        """Test service task"""
        task = Task(name="Service Task", task_type=TaskType.SERVICE, implementation="http://api.example.com/service", timeout_seconds=30)

        assert task.task_type == TaskType.SERVICE
        assert task.implementation == "http://api.example.com/service"
        assert task.timeout_seconds == 30

    def test_multi_instance_task(self):
        """Test multi-instance task"""
        task = Task(name="Multi Task", is_multi_instance=True, multi_instance_collection="items", is_sequential=False)

        assert task.is_multi_instance is True
        assert task.multi_instance_collection == "items"
        assert task.is_sequential is False


class TestSubProcess:
    """Test BPMN subprocess"""

    def test_subprocess_creation(self):
        """Test subprocess creation"""
        subprocess = SubProcess(name="Sub Process", flow_nodes=["task1", "task2", "gateway1"], sequence_flows=["flow1", "flow2"])

        assert subprocess.name == "Sub Process"
        assert len(subprocess.flow_nodes) == 3
        assert len(subprocess.sequence_flows) == 2
        assert subprocess.is_transaction is False

    def test_event_subprocess(self):
        """Test event subprocess"""
        subprocess = SubProcess(name="Event Sub", is_event_subprocess=True, triggered_by_event=True)

        assert subprocess.is_event_subprocess is True
        assert subprocess.triggered_by_event is True


class TestGateway:
    """Test BPMN gateways"""

    def test_exclusive_gateway(self):
        """Test exclusive gateway"""
        gateway = Gateway(name="XOR Gateway", gateway_type=GatewayType.EXCLUSIVE, default_flow="flow_default")

        assert gateway.name == "XOR Gateway"
        assert gateway.gateway_type == GatewayType.EXCLUSIVE
        assert gateway.default_flow == "flow_default"
        assert gateway.gateway_direction == "unspecified"

    def test_parallel_gateway(self):
        """Test parallel gateway"""
        gateway = Gateway(name="AND Gateway", gateway_type=GatewayType.PARALLEL, gateway_direction="diverging")

        assert gateway.gateway_type == GatewayType.PARALLEL
        assert gateway.gateway_direction == "diverging"


class TestSequenceFlow:
    """Test sequence flows"""

    def test_sequence_flow(self):
        """Test basic sequence flow"""
        flow = SequenceFlow(name="Flow 1", source_ref="task1", target_ref="task2")

        assert flow.name == "Flow 1"
        assert flow.source_ref == "task1"
        assert flow.target_ref == "task2"
        assert flow.is_default is False
        assert flow.tokens_passed == 0

    def test_conditional_flow(self):
        """Test conditional sequence flow"""
        flow = SequenceFlow(name="Conditional Flow", source_ref="gateway1", target_ref="task3", condition_expression="${amount > 1000}")

        assert flow.condition_expression == "${amount > 1000}"


class TestPool:
    """Test BPMN pools"""

    def test_pool_creation(self):
        """Test pool creation"""
        pool = Pool(name="Organization Pool", participant_ref="org_123", process_ref="process_456", lanes=["lane1", "lane2"])

        assert pool.name == "Organization Pool"
        assert pool.participant_ref == "org_123"
        assert pool.process_ref == "process_456"
        assert len(pool.lanes) == 2
        assert pool.is_executable is True


class TestLane:
    """Test BPMN lanes"""

    def test_lane_creation(self):
        """Test lane creation"""
        lane = Lane(name="Department Lane", pool_id="pool_123", performer_ref="dept_role", flow_node_refs=["task1", "task2", "gateway1"])

        assert lane.name == "Department Lane"
        assert lane.pool_id == "pool_123"
        assert lane.performer_ref == "dept_role"
        assert len(lane.flow_node_refs) == 3


class TestProcess:
    """Test BPMN process"""

    def test_process_creation(self):
        """Test process creation"""
        process = Process(
            name="Test Process", process_type=ProcessType.PRIVATE, version="1.0.0", flow_nodes=["start", "task1", "end"], sequence_flows=["flow1", "flow2"]
        )

        assert process.name == "Test Process"
        assert process.process_type == ProcessType.PRIVATE
        assert process.version == "1.0.0"
        assert process.is_executable is True
        assert process.is_latest is True
        assert len(process.flow_nodes) == 3
        assert len(process.sequence_flows) == 2

    def test_process_metrics(self):
        """Test process with metrics"""
        process = Process(name="Metrics Process", instances_created=100, instances_completed=95, average_duration_ms=30000)

        assert process.instances_created == 100
        assert process.instances_completed == 95
        assert process.average_duration_ms == 30000


class TestProcessInstance:
    """Test process instance"""

    def test_instance_creation(self):
        """Test process instance creation"""
        instance = ProcessInstance(process_id="process_123", process_version="1.0.0", correlation_key="order_456", business_key="ORDER-2024-001")

        assert instance.process_id == "process_123"
        assert instance.process_version == "1.0.0"
        assert instance.instance_id.startswith("instance_")
        assert is_uuid7(instance.instance_id.replace("instance_", ""))
        assert instance.state == "created"
        assert instance.correlation_key == "order_456"
        assert instance.business_key == "ORDER-2024-001"

    def test_instance_with_variables(self):
        """Test instance with process variables"""
        instance = ProcessInstance(
            process_id="process_123", process_version="1.0.0", variables={"customer": "John Doe", "amount": 1500, "priority": "high"}, active_tasks=["task_789"]
        )

        assert instance.variables["customer"] == "John Doe"
        assert instance.variables["amount"] == 1500
        assert len(instance.active_tasks) == 1


class TestGoal:
    """Test goal-oriented BPMN extension"""

    def test_goal_creation(self):
        """Test goal creation"""
        goal = Goal(
            name="Complete Order",
            goal_type="achievement",
            priority=8,
            satisfaction_condition="order.status == 'completed'",
            supporting_tasks=["validate", "process", "ship"],
        )

        assert goal.name == "Complete Order"
        assert goal.goal_type == "achievement"
        assert goal.priority == 8
        assert len(goal.supporting_tasks) == 3
        assert goal.is_satisfied is False
        assert goal.satisfaction_level == 0.0


class TestMessage:
    """Test BPMN messages"""

    def test_message_creation(self):
        """Test message creation"""
        message = Message(
            name="Order Message",
            message_type="request",
            payload={"order_id": "123", "items": ["A", "B"]},
            correlation_key="order_123",
            source_participant="customer",
            target_participant="supplier",
        )

        assert message.name == "Order Message"
        assert message.message_type == "request"
        assert message.payload["order_id"] == "123"
        assert message.correlation_key == "order_123"
        assert message.is_consumed is False
        assert isinstance(message.timestamp, datetime)
