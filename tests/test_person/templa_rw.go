package main55

import "fmt"

// 定义抽象接口，只包含可能被子类重写的方法
type AbstractInterface interface {
    ConcreteStep() // 子类可以重写的具体方法
}

// 定义模板结构体，包含模板方法和通用步骤实现
type Template struct {
    impl AbstractInterface // 具体实现者
}

// 模板方法，定义算法骨架
func (t *Template) TemplateMethod() {
    t.Step1()
    t.impl.ConcreteStep() // 调用可能被子类重写的方法
    t.Step2()
    t.Step3()
}

// 通用步骤 Step1 - 父类实现
func (t *Template) Step1() {
    fmt.Println("Template Step1")
}

// 通用步骤 Step2 - 父类实现
func (t *Template) Step2() {
    fmt.Println("Template Step2")
}

// 通用步骤 Step3 - 父类实现
func (t *Template) Step3() {
    fmt.Println("Template Step3")
}

// 默认的具体步骤实现
func (t *Template) ConcreteStep() {
    fmt.Println("Template ConcreteStep")
}

// 具体子类 A
type ConcreteClassA struct {
    *Template
}

// 创建新的ConcreteClassA实例的工厂方法
func NewConcreteClassA() *ConcreteClassA {
    // concrete := &ConcreteClassA{}
    // concrete.Template = &Template{impl: concrete}

	concrete := &ConcreteClassA{Template: &Template{}}
	concrete.Template.impl = concrete
    return concrete
}

// 重写具体方法 ConcreteStep
func (c *ConcreteClassA) ConcreteStep() {
    fmt.Println("ConcreteClassA ConcreteStep")
}

// 具体子类 B
type ConcreteClassB struct {
    *Template
}

// 创建新的ConcreteClassB实例的工厂方法
func NewConcreteClassB() *ConcreteClassB {
    concrete := &ConcreteClassB{}
    concrete.Template = &Template{impl: concrete}
    return concrete
}

// ConcreteClassB 不重写 ConcreteStep，将使用父类的默认实现

func main() {
    // 创建 ConcreteClassA 实例
    a := NewConcreteClassA()
    fmt.Println("ConcreteClassA:")
    a.TemplateMethod()

    // 创建 ConcreteClassB 实例
    b := NewConcreteClassB()
    fmt.Println("\nConcreteClassB:")
    b.TemplateMethod()
}