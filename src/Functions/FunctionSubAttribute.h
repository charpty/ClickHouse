#pragma once
#include <string>
#include <unistd.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context_fwd.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/*
 * CK中编写子文档UDF示例
 * 由于是演示，为了代码整洁清晰，很多校验就不在代码中体现了
 * 
 * 参数1：要取哪个子文档的属性
 * 参数2：子文档约束条件
 * 后续参数：lambda表达式的入参（均为子文档字段）
 * select sub_attribute(['r1','r2'], 
 *                     (x, y) -> ((x != 'a') AND (y != '')), 
 *                     ['a', 'b'], ['1', '2'])
 * 结果：'r2'
 */
class FunctionSubAttribute : public IFunction
{
public:
    static constexpr auto name = "sub_attribute";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSubAttribute>(); }

    String getName() const override { return name; }

    // 声明为可变参数的UDF
    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    // 当UDF的入参中存在lambda时会调用该函数
    // 默认解析后会认为后面的“数组”入参即为lambda入参
    // 但此处我们并不需要“数组”作为lambda的入参，而是需要“数组”中的元素作为入参，所以需要展开
    void getLambdaArgumentTypes(DataTypes & arguments) const override
    {
        // TODO 这里需要做各种参数检验，包括是否为参数个数，数组类型等
        DataTypes nested_types(arguments.size() - 2);
        for (size_t i = 0; i < nested_types.size(); ++i)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(&*arguments[i + 2]);
            nested_types[i] = recursiveRemoveLowCardinality(array_type->getNestedType());
        }

        const DataTypeFunction * function_type = checkAndGetDataType<DataTypeFunction>(arguments[1].get());
        // 校验lambda的入参个数和传入的参数个数是否匹配
        if (!function_type || function_type->getArgumentTypes().size() != nested_types.size())
        {
            throw Exception("argument ... not match for...", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        // 修改lambda函数类型
        arguments[1] = std::make_shared<DataTypeFunction>(nested_types);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        // TODO 同样需要类型和合法性校验
        const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].type.get());
        DataTypePtr nested_type = array_type->getNestedType();
        // 我们的返回值就是第一个子文档列的某一行，即第一个入参数组的某一个元素
        return nested_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        (void)result_type;
        (void)input_rows_count;
        // CK是利用SSE等做并行计算的，为适配逻辑，UDF也不是来一行计算一次，而是先将这个UDF的结果计算为Column

        // 组装lambda所需的入参列
        ColumnsWithTypeAndName arrays;
        arrays.reserve(arguments.size() - 2);
        const size_t args_start_index = 2;
        // 主要是要知道有多少行，UDF的计算结果行数相同
        const ColumnArray * column_first_array = nullptr;
        for (size_t i = args_start_index; i < arguments.size(); ++i)
        {
            const auto & array_with_type_and_name = arguments[i];
            ColumnPtr column_array_ptr = array_with_type_and_name.column;
            const auto * column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());

            // 如果传入的不是列名而是常量，则需使用checkAndGetColumnConst
            if (!column_array)
            {
                const ColumnConst * column_const_array = checkAndGetColumnConst<ColumnArray>(column_array_ptr.get());
                // TODO 所有转换的地方都需要校验下
                column_array_ptr = recursiveRemoveLowCardinality(column_const_array->convertToFullColumn());
                column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());
            }
            if (args_start_index == i)
            {
                column_first_array = column_array;
            }

            // TODO 校验数组类型及各数组大小
            const auto * array_type = checkAndGetDataType<DataTypeArray>(array_with_type_and_name.type.get());
            arrays.emplace_back(ColumnWithTypeAndName(
                column_array->getDataPtr(), recursiveRemoveLowCardinality(array_type->getNestedType()), array_with_type_and_name.name));
        }

        // 我们的第二个参数是个lambda
        const auto * column_function = typeid_cast<const ColumnFunction *>(arguments[1].column.get());
        if (!column_function)
        {
            throw Exception("Second argument " + getName() + " must be a function.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        // 把之前组装好的入参传给lambda
        auto replicated_column_function_ptr = IColumn::mutate(column_function->replicate(column_first_array->getOffsets()));
        auto * replicated_column_function = typeid_cast<ColumnFunction *>(replicated_column_function_ptr.get());
        replicated_column_function->appendArguments(arrays);

        // 计算lambda表达式的结果
        ColumnPtr lambda_result = replicated_column_function->reduce().column;
        if (lambda_result->lowCardinality())
        {
            lambda_result = lambda_result->convertToFullColumnIfLowCardinality();
        }
        
        // TODO lambda计算结果类型校验
        // 我们知道我们的lambda计算结果为bool列
        const auto * column_filter = typeid_cast<const ColumnUInt8 *>(&*lambda_result);

        // 第一个参数代表了要取哪个子文档属性
        ColumnPtr column_array_ptr = arguments[0].column;
        const auto * array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());
        // 如果传入的不是列名而是常量，则需使用checkAndGetColumnConst
        if (!array)
        {
            const ColumnConst * column_const_array = checkAndGetColumnConst<ColumnArray>(column_array_ptr.get());
            // TODO 所有转换的地方都需要校验下
            column_array_ptr = recursiveRemoveLowCardinality(column_const_array->convertToFullColumn());
            array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());
        }

        // filter就是个bool数组，代表多列数组入参的每一行（相同位置）是否满足lambda条件
        const auto & filter = column_filter->getData();
        const auto & offsets = array->getOffsets();
        const auto & data = array->getData();
        auto out = data.cloneEmpty();
        out->reserve(data.size());

        size_t pos{};
        for (auto offset : offsets)
        {
            auto find = false;
            for (; pos < offset; ++pos)
            {
                if (filter[pos])
                {
                    out->insert(data[pos]);
                    find = true;
                    pos = offset;
                    break;
                }
            }
            if (!find)
            {
                // 未找到也需要插默认值，保证offset正确
                out->insertDefault();
            }
        }
        return out;
    }
};

}
