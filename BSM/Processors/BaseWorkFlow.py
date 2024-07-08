from BSM.Processors.BaseWorkflowProcessor import TestBaseWorkflowProcessor


class Workflow:
    def __init__(self):
        self.modules = {}
        self.root_module = None
        self.execution_count = {}

    def add_module(self, module_name, module, upstream_module_name=None, flag=None):
        self.modules[module_name] = module
        self.execution_count[module.__class__.__name__] = 0  # 初始化每个模块的执行次数为0
        if not upstream_module_name:
            if self.root_module:
                raise ValueError('root module exists, please set module as submodule or change root module')
            else:
                self.root_module = module
        if upstream_module_name:
            upstream_module = self.modules[upstream_module_name]
            if flag == 'positive':
                upstream_module.positive_next_module = module
            elif flag == 'negative':
                upstream_module.negative_next_module = module

    def run_workflow(self, data):
        current_module = self.root_module
        while current_module:
            data, flag = current_module.process(data)
            self.execution_count[current_module.__class__.__name__] += 1  # 增加模块的执行次数
            current_module = current_module.positive_next_module if flag else current_module.negative_next_module
        return data, self.execution_count

# 创建一个工作流
workflow = Workflow()

# 添加第一个模块
first_module = TestBaseWorkflowProcessor()
workflow.add_module("Module0", first_module)

# 添加剩余的模块
for i in range(1, 10):
    module_name = f"Module{i}"
    module = TestBaseWorkflowProcessor()
    workflow.add_module(module_name, module, upstream_module_name=f"Module{i-1}", flag='positive')

# 运行工作流
data = "test"
processed_data, execution_count = workflow.run_workflow(data)

print(processed_data, execution_count)