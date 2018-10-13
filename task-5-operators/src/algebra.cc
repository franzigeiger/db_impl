#include "moderndbs/algebra.h"
#include <cassert>
#include <functional>
#include <string>
#include <unordered_set>


namespace moderndbs {
namespace iterator_model {


/// This can be used to store registers in an `std::unordered_map` or
/// `std::unordered_set`. Examples:
///
/// std::unordered_map<Register, int, RegisterHasher> map_from_reg_to_int;
/// std::unordered_set<Register, RegisterHasher> set_of_regs;
/*struct RegisterHasher {
    uint64_t operator()(const Register& r) const {
        return r.get_hash();
    }
};*/


/// This can be used to store vectors of registers (which is how tuples are
/// represented) in an `std::unordered_map` or `std::unordered_set`. Examples:
///
/// std::unordered_map<std::vector<Register>, int, RegisterVectorHasher> map_from_tuple_to_int;
/// std::unordered_set<std::vector<Register>, RegisterVectorHasher> set_of_tuples;


        static std::vector<Register> transform(std::vector<Register *> ingoing){
            std::vector<Register> registers;

            for(Register* reg : ingoing){
                registers.push_back(*reg);
            }
            return registers;
        }

        static std::vector<Register *> transformToPointer(std::vector<Register> &ingoing){
            std::vector<Register*> outgoing;

            for(Register &reg : ingoing){
                outgoing.push_back(&reg);
            }

            return outgoing;
        }


Register Register::from_int(int64_t value) {
    Register newRegister;
    newRegister.isInt = true;
    newRegister.integer = value;
    return newRegister;
}


Register Register::from_string(const std::string& value) {
    Register newRegister;
    newRegister.isInt = false;
    newRegister.string = value;
    return newRegister;
}


Register::Type Register::get_type() const {
    if(this->isInt) {
        return Type::INT64;
    } else {
        return Type::CHAR16;
    }
}


int64_t Register::as_int() const {
        return integer;
}


std::string Register::as_string() const {
    return string;
}


uint64_t Register::get_hash() const {
    return isInt?  std::hash<uint64_t >()(integer) : std::hash<std::string>()(string);
}


bool operator==(const Register& r1, const Register& r2) {
    if(r1.isInt == r2.isInt){
        if(r1.isInt==true){
            return r1.integer == r2.integer;
        }else {
            return r1.string == r2.string;
        }
    }
    return false;
}


bool operator!=(const Register& r1, const Register& r2) {
    if(r1.isInt == r2.isInt){
        if(r1.isInt==true){
            return r1.integer != r2.integer;
        }else {
            return r1.string != r2.string;
        }
    }
    return true;
}


bool operator<(const Register& r1, const Register& r2) {
    assert(r1.get_type() == r2.get_type());
    if(r1.isInt == r2.isInt){
        if(r1.isInt==true){
            return r1.integer < r2.integer;
        }else {
            return r1.string.compare(r2.string) < 0  ? true : false;

        }
    }
    return false;
}


bool operator<=(const Register& r1, const Register& r2) {
    assert(r1.get_type() == r2.get_type());
    if(r1.isInt == r2.isInt){
        if(r1.isInt==true){
            return r1.integer < r2.integer;
        }else {
            return r1.string.compare(r2.string) <= 0 ? true : false;

        }
    }
    return false;
}


bool operator>(const Register& r1, const Register& r2) {
    assert(r1.get_type() == r2.get_type());
    if(r1.isInt == r2.isInt){
        if(r1.isInt==true){
            return r1.integer > r2.integer;
        }else {
            return r1.string.compare(r2.string) > 0 ? true : false;

        }
    }
    return false;
}


bool operator>=(const Register& r1, const Register& r2) {
    assert(r1.get_type() == r2.get_type());
    if(r1.isInt == r2.isInt){
        if(r1.isInt==true){
            return r1.integer >= r2.integer;
        }else {
            return r1.string.compare(r2.string) >= 0 ? true : false;

        }
    }
    return false;
}


Print::Print(Operator& input, std::ostream& stream) : UnaryOperator(input) {
    this->stream=&stream;
}


Print::~Print() = default;


void Print::open() {
   input->open();
}


bool Print::next() {
   if(input->next()){
       std::vector<Register *> ingoing = input->get_output();
       size_t last =0;
       for(Register* registerItem :  ingoing){
           if(registerItem->get_type() == Register::Type::INT64){
               *stream << registerItem->as_int();

           }else {
               *stream << registerItem->as_string();
           }
            if(last++ != ingoing.size()-1){
                *stream << ',';
           }

       }
       stream->put('\n') ;
       return true;
   }
   return false;
}


void Print::close() {
        stream->clear();
        input->close();
}


std::vector<Register*> Print::get_output() {
    // Print has no output
    return {};
}


Projection::Projection(Operator& input, std::vector<size_t> attr_indexes)
: UnaryOperator(input) {
    this->attr_indexes = attr_indexes;
}


Projection::~Projection() = default;


void Projection::open() {
    input->open();
}


bool Projection::next() {
    return input->next();
}


void Projection::close() {
    input->close();
}


std::vector<Register*> Projection::get_output() {
    std::vector<Register*> rawColumns = input->get_output();
    std::vector<Register*> projection (attr_indexes.size()-1);

    for(size_t column :attr_indexes) {
       projection.push_back( rawColumns.at(column));
    }
    return projection;
}


Select::Select(Operator& input, PredicateAttributeInt64 predicate)
: UnaryOperator(input) {
    this->predicateType=0;
    this->intPredicate=predicate;
}


Select::Select(Operator& input, PredicateAttributeChar16 predicate)
: UnaryOperator(input) {
    this->predicateType=1;
    this->stringPredicate=predicate;
}


Select::Select(Operator& input, PredicateAttributeAttribute predicate)
: UnaryOperator(input), predicate(predicate) {
    this->predicateType=2;
}


Select::~Select() = default;


void Select::open() {
    input->open();
}


bool Select::next() {
    while(input->next()){
        std::vector<Register *> ingoing = input->get_output();
        Register rightSide;
        Register leftSide;
        PredicateType  type;
        switch(predicateType){
            case 0:
                rightSide = Register::from_int(intPredicate.constant);
                leftSide = *ingoing.at(intPredicate.attr_index);
                type = intPredicate.predicate_type;
                break;
            case 1:
                rightSide = Register::from_string(stringPredicate.constant);
                leftSide = *ingoing.at(stringPredicate.attr_index);
                type = stringPredicate.predicate_type;
                break;
            case 2:
                type=predicate.predicate_type;
                leftSide =*ingoing.at(predicate.attr_left_index);
                rightSide = *ingoing.at(predicate.attr_right_index);
                break;
            default:
                return false;
        }


        bool result;
        switch(type){
            case PredicateType::EQ:
                result = (leftSide == rightSide);
                break;
            case PredicateType::GE:
                result = (leftSide >= rightSide);
                break;
            case PredicateType::GT:
                result = (leftSide > rightSide);
                break;
            case PredicateType::LE:
                result = (leftSide <= rightSide);
                break;
            case PredicateType::LT:
                result = (leftSide < rightSide);
                break;
            case PredicateType::NE:
                result = (leftSide != rightSide);
                break;
        }

        if(result){
            return true;
        }
    }
    return false;
}


void Select::close() {
    input->close();
}


std::vector<Register*> Select::get_output() {
    return input->get_output();
}


Sort::Sort(Operator& input, std::vector<Criterion> criteria)
: UnaryOperator(input) , criteria(std::move(criteria)){
}


Sort::~Sort() = default;


void Sort::open() {
    input->open();

    while(input->next()){
        std::vector<Register> registers = transform(input->get_output());
        tuples.push_back(registers);
    }


    std::sort(tuples.begin(), tuples.end(), [this](const std::vector<Register> & left, const std::vector<Register> & right){
        int criteriaIndex =0;
        while(left.at(criteria.at(criteriaIndex).attr_index) == right.at(criteria.at(criteriaIndex).attr_index) && criteriaIndex < criteria.size()-1){
            criteriaIndex++;
        }

        bool result = (left.at(this->criteria.at(criteriaIndex).attr_index) < right.at(criteria.at(criteriaIndex).attr_index));

        return criteria.at(criteriaIndex).desc ? !result : result;
    });


}

    bool Sort::next() {
        return index < tuples.size();
    }

    std::vector<Register*> Sort::get_output() {
        return transformToPointer(tuples.at(index++));
    }


    void Sort::close() {
        input->close();
}




HashJoin::HashJoin(
    Operator& input_left,
    Operator& input_right,
    size_t attr_index_left,
    size_t attr_index_right
) : BinaryOperator(input_left, input_right) {
    this->input_left=&input_left;
    this->input_right=&input_right;
    this->attr_index_left=attr_index_left;
    this->attr_index_right=attr_index_right;
}


HashJoin::~HashJoin() = default;


void HashJoin::open() {
    input_left->open();
    input_right->open();

    while(input_left->next()){
        std::vector<Register*> ingoing=input_left->get_output();

        std::vector<Register> registers = transform(ingoing);

        std::pair<Register, std::vector<Register>> pair= std::make_pair(registers.at(attr_index_left), registers);
        hashTable[registers.at(attr_index_left)]= registers;
    }
}


bool HashJoin::next() {
    while(input_right->next()){
        std::vector<Register> ingoing = transform(input_right->get_output());
        if(hashTable.count(ingoing.at(attr_index_right))){
            std::vector<Register> leftTuple= (hashTable[ingoing.at(attr_index_right)]);
            for(size_t i =0; i< ingoing.size() ; i++){
                leftTuple.push_back(ingoing.at(i));
            }

            current= leftTuple;
            return true;
        }
    }
    return false;
}


void HashJoin::close() {
    input_right->close();
    input_left->close();
}


std::vector<Register*> HashJoin::get_output() {
    return transformToPointer(current);
}


HashAggregation::HashAggregation(
    Operator& input,
    std::vector<size_t> group_by_attrs,
    std::vector<AggrFunc> aggr_funcs
) : UnaryOperator(input) {
   this->aggr_funcs=aggr_funcs;
   this->group_by_attrs=group_by_attrs;
}


HashAggregation::~HashAggregation() = default;


Register HashAggregation::aggregate(AggrFunc function , Register &base, Register &add){
    if(add.get_type()==Register::Type::INT64) {
        uint64_t  value=base.as_int();
        uint64_t addInt =add.as_int();;
        switch (function.func){
            case AggrFunc::COUNT:
                value++;
                break;
            case AggrFunc::MAX:
                value = value < addInt ? addInt : value;
                break;
            case AggrFunc::MIN:
                value = value > addInt ? addInt : value;
                break;
            case AggrFunc::SUM:
                value +=  addInt;
                break;
            default:
                break;
        }
        base = *new Register(Register::from_int(value));
    }else {
        std::string value=base.as_string();
        std::string addString=add.as_string();
        switch (function.func) {
            case AggrFunc::MAX:
                value = value < addString ? addString : value;
                break;
            case AggrFunc::MIN:
                value = value > addString ? addString : value;
                break;
            default:
                break;
        }
        base =*new Register( Register::from_string(value));
    }
    return base;
}

void HashAggregation::open() {
   input->open();
    std::unordered_map<std::vector<Register>, std::vector<Register>, RegisterVectorHasher> hashTable;

   while(input->next()){
       std::vector<Register> ingoing = transform(input->get_output());
       std::vector<Register> group ;
       if(group_by_attrs.size()==0){
            group.push_back(Register::from_int(0));
       } else{
           for(size_t index : this->group_by_attrs){
               group.push_back(ingoing.at(index));
           }
       }

            if(hashTable.count(group)){
               std::vector<Register>& reg = hashTable[group] ;
               for(size_t i=0; i< aggr_funcs.size() ; i++) {
                   reg[i] = aggregate(aggr_funcs[i], reg[i], ingoing.at(aggr_funcs[i].attr_index));
               }

            } else {
                std::vector<Register> reg ;
                for(size_t i=0; i< aggr_funcs.size() ; i++) {
                    Register addValue = ingoing.at(aggr_funcs[i].attr_index);
                    Register newAggregate;
                    if (aggr_funcs[i].func == AggrFunc::COUNT || aggr_funcs[i].func == AggrFunc::SUM) {
                        newAggregate = Register::from_int(0);
                    } else if (addValue.get_type() == Register::Type::INT64) {
                        newAggregate = Register::from_int(addValue.as_int());
                    } else {
                        newAggregate = Register::from_string(addValue.as_string());
                    }
                    ;
                    reg.push_back(aggregate(aggr_funcs[i], newAggregate, addValue));
                }
                hashTable[group] = reg;
            }


   }
    for(std::pair<std::vector<Register>, std::vector<Register>> group : hashTable){

        const std::vector<moderndbs::iterator_model::Register> key= group.first;
        std::vector<Register> value = hashTable[key];

        if(group_by_attrs.size()==0){
            result.push_back(value);
        }else{
            std::vector<Register> together = key;
            together.insert(together.end(),value.begin(), value.end());
            result.push_back(together);
        }

    }


}


bool HashAggregation::next() {
        if(resultIndex == result.size()){
            return false;
        }
        return true;
};


void HashAggregation::close() {
    input->close();

}


std::vector<Register*> HashAggregation::get_output() {
    return transformToPointer(result[resultIndex++]);
}


Union::Union(Operator& input_left, Operator& input_right)
: BinaryOperator(input_left, input_right) {
    // TODO: add your implementation here
}


Union::~Union() = default;


void Union::open() {
    // TODO: add your implementation here
}


bool Union::next() {
    // TODO: add your implementation here
    return false;
}


std::vector<Register*> Union::get_output() {
    // TODO: add your implementation here
    return {};
}


void Union::close() {
    // TODO: add your implementation here
}


UnionAll::UnionAll(Operator& input_left, Operator& input_right)
: BinaryOperator(input_left, input_right) {
    // TODO: add your implementation here
}


UnionAll::~UnionAll() = default;


void UnionAll::open() {
    // TODO: add your implementation here
}


bool UnionAll::next() {
    // TODO: add your implementation here
    return false;
}


std::vector<Register*> UnionAll::get_output() {
    // TODO: add your implementation here
    return {};
}


void UnionAll::close() {
    // TODO: add your implementation here
}


Intersect::Intersect(Operator& input_left, Operator& input_right)
: BinaryOperator(input_left, input_right) {
}


Intersect::~Intersect() = default;


void Intersect::open() {
    input_left->open();
    std::unordered_set<std::vector<Register>, RegisterVectorHasher> left;
    std::unordered_set<std::vector<Register>, RegisterVectorHasher>  intersections;
    while(input_left->next()){
        left.insert(transform(input_left->get_output()));
    }

    input_right->open();
    while(input_right->next()){
        std::vector<Register> right= transform(input_right->get_output());
        if(left.find(right)!= left.end()){
            intersections.insert(right);
        }
    }

    for(auto elem : intersections){
        result.push_back(elem);
    }

}


bool Intersect::next() {
    if(resultIndex < result.size()){
        return true;
    }
    return false;
}


std::vector<Register*> Intersect::get_output() {
    return transformToPointer(result[resultIndex++]);
}


void Intersect::close() {
    input_right->close();
    input_left->close();
}


IntersectAll::IntersectAll(Operator& input_left, Operator& input_right)
: BinaryOperator(input_left, input_right) {

}


IntersectAll::~IntersectAll() = default;


void IntersectAll::open() {
    input_left->open();
    std::unordered_set<std::vector<Register>, RegisterVectorHasher> left;
    std::unordered_set<std::vector<Register>, RegisterVectorHasher>  intersections;
    while(input_left->next()){
        left.insert(transform(input_left->get_output()));
    }

    input_right->open();
    while(input_right->next()){
        std::vector<Register> right= transform(input_right->get_output());
        if(left.find(right)!= left.end()){
            result.push_back(right);
        }
    }
}


bool IntersectAll::next() {
    if(resultIndex < result.size()){
        return true;
    }
    return false;
}


std::vector<Register*> IntersectAll::get_output() {
    return transformToPointer(result[resultIndex++]);
}


void IntersectAll::close() {
    input_right->close();
    input_left->close();
}


Except::Except(Operator& input_left, Operator& input_right)
: BinaryOperator(input_left, input_right) {
    // TODO: add your implementation here
}


Except::~Except() = default;


void Except::open() {
    input_right->open();
    std::unordered_set<std::vector<Register>, RegisterVectorHasher> right;
    std::unordered_set<std::vector<Register>, RegisterVectorHasher>  except;
    while(input_right->next()){
        right.insert(transform(input_right->get_output()));
    }

    input_left->open();
    while(input_left->next()){
        std::vector<Register> left= transform(input_left->get_output());
        if(right.find(left)== right.end()){
            except.insert(left);
        }
    }

    for(auto elem : except){
        result.push_back(elem);
    }
}


bool Except::next() {
    if(resultIndex < result.size()){
        return true;
    }
    return false;
}


std::vector<Register*> Except::get_output() {
    return transformToPointer(result[resultIndex++]);
}


void Except::close() {
    input_right->close();
    input_left->close();
}


ExceptAll::ExceptAll(Operator& input_left, Operator& input_right)
: BinaryOperator(input_left, input_right) {
    // TODO: add your implementation here
}


ExceptAll::~ExceptAll() = default;


void ExceptAll::open() {
    input_right->open();
    std::unordered_map<std::vector<Register>,size_t, RegisterVectorHasher> right;
    std::unordered_set<std::vector<Register>, RegisterVectorHasher>  intersections;
    while(input_right->next()){
        std::vector<Register> rightTuple = transform(input_right->get_output());
        if(right.count(rightTuple)){
            right[rightTuple]++;
        } else{
            right[rightTuple]=1;
        };
    }

    input_left->open();
    while(input_left->next()){
        std::vector<Register> left= transform(input_left->get_output());
        if(right.find(left)== right.end()){
            result.push_back(left);
        } else {
            if(right[left]==0){
                result.push_back(left);
            } else{
                right[left]--;
            }
        }
    }
}


bool ExceptAll::next() {
    if(resultIndex < result.size()){
        return true;
    }
    return false;
}


std::vector<Register*> ExceptAll::get_output() {
    return transformToPointer(result[resultIndex++]);
}


void ExceptAll::close() {
    input_right->close();
    input_left->close();
}

}  // namespace iterator_model
}  // namespace moderndbs
