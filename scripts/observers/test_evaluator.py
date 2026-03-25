class TestEvaluator:
    @staticmethod
    def evaluate_condition(metric, operator, threshold):
        mapping = {
            "<=": lambda m, t: m <= t,
            ">=": lambda m, t: m >= t,
            "=":  lambda m, t: m == t,
            "==": lambda m, t: m == t,
            "<":  lambda m, t: m < t,
            ">":  lambda m, t: m > t
        }
        
        operation = mapping.get(operator)
        if not operation:
            return "fail"
            
        return "pass" if operation(metric, threshold) else "fail"