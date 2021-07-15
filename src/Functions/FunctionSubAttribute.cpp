#include <Functions/FunctionSubAttribute.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

void registerFunctionSubAttribute(FunctionFactory & factory)
{
    // 在这也可以注册别名
    factory.registerFunction<FunctionSubAttribute>();
}

}
