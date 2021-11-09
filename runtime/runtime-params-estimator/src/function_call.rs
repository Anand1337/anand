use crate::cases::ratio_to_gas_signed;
use crate::testbed_runners::{end_count, start_count, Consumed, GasMetric};
use crate::vm_estimator::{create_context, least_squares_method, least_squares_method_2};
use nalgebra::{DMatrix, DVector, Matrix, Matrix2x3, MatrixXx4, RowVector, Vector, Vector2};
use near_logger_utils::init_test_logger;
use near_primitives::config::VMConfig;
use near_primitives::contract::ContractCode;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::{CompiledContractCache, ProtocolVersion};
use near_store::{create_store, StoreCompiledContractCache};
use near_test_contracts::{
    aurora_contract, get_aurora_330_data, get_aurora_contract_data, get_aurora_small_contract_data,
    get_multisig_contract_data, get_rs_contract_data, get_voting_contract_data,
    many_functions_contract,
};
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::ExtCostsConfig;
use near_vm_runner::cache;
use near_vm_runner::prepare::{get_functions_number, prepare_contract};
use near_vm_runner::runner::compile_w2;
use near_vm_runner::{precompile_contract, run_vm, VMKind};
use nearcore::get_store_path;
use num_rational::Ratio;
use std::cmp::max;
use std::collections::HashMap;
use std::fmt::Write;
use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::str;
use std::sync::Arc;
use walrus::ir::*;
use walrus::{ExportItem, FunctionBuilder, ImportKind, Module, ModuleConfig, ValType};

const REPEATS: u64 = 20;

fn get_func_number(contract: &ContractCode) -> usize {
    let module =
        cache::wasmer0_cache::compile_module_cached_wasmer0(&contract, &VMConfig::default(), None)
            .unwrap()
            .unwrap();
    let module_info = module.info();

    println!("-------");
    println!("{}", module_info.memories.len());
    println!("{}", module_info.globals.len());
    println!("{}", module_info.tables.len());
    println!("{}", module_info.imported_functions.len());
    println!("{}", module_info.imported_memories.len());
    println!("{}", module_info.imported_tables.len());
    println!("{}", module_info.imported_globals.len());
    println!("{}", module_info.exports.map.len());
    println!("{}", module_info.data_initializers.len());
    println!("{}", module_info.elem_initializers.len());
    println!("{}", module_info.func_assoc.len());
    println!("{}", module_info.signatures.len());
    println!("{}", module_info.backend.len());
    println!("{}", module_info.custom_sections.len());
    println!("-------");
    let namespace_table = &module_info.namespace_table;
    let table = &module_info.name_table;
    let imported_funcs: Vec<_> = module_info
        .imported_functions
        .values()
        .map(|i| (namespace_table.get(i.namespace_index), table.get(i.name_index)))
        .collect();
    println!("{:?}", imported_funcs);

    module_info.func_assoc.len()
}

fn get_complexity(contract: &ContractCode) -> usize {
    let module =
        cache::wasmer0_cache::compile_module_cached_wasmer0(&contract, &VMConfig::default(), None)
            .unwrap()
            .unwrap();
    let module_info = module.info();

    println!("-------");
    println!("{}", module_info.memories.len());
    println!("{}", module_info.globals.len());
    println!("{}", module_info.tables.len());
    println!("{}", module_info.imported_functions.len());
    println!("{}", module_info.imported_memories.len());
    println!("{}", module_info.imported_tables.len());
    println!("{}", module_info.imported_globals.len());
    println!("{}", module_info.exports.map.len());
    println!("{}", module_info.data_initializers.len());
    println!("{}", module_info.elem_initializers.len());
    println!("{}", module_info.func_assoc.len());
    println!("{}", module_info.signatures.len());
    println!("{}", module_info.backend.len());
    println!("{}", module_info.custom_sections.len());
    println!("-------");
    let namespace_table = &module_info.namespace_table;
    let table = &module_info.name_table;
    let imported_funcs: Vec<_> = module_info
        .imported_functions
        .values()
        .map(|i| (namespace_table.get(i.namespace_index), table.get(i.name_index)))
        .collect();
    println!("{:?}", imported_funcs);

    module_info.signatures.len()
        + module_info.imported_functions.len()
        + module_info.imported_memories.len()
        + module_info.imported_tables.len()
        + module_info.imported_globals.len()
        + module_info.func_assoc.len()
        + module_info.tables.len()
        + module_info.memories.len()
        + module_info.globals.len()
        + module_info.exports.map.len()
        + module_info.elem_initializers.len()
    + 0 // code section
    + module_info.data_initializers.len()
}

#[allow(dead_code)]
fn test_function_call_try_complexity_metric(metric: GasMetric, vm_kind: VMKind) {
    // let (mut args_len_xs, mut code_len_xs, mut funcs_xs) = (vec![], vec![], vec![]);
    // let mut ys = vec![];
    let mut rows = 0;
    let mut data = Vec::new();

    // let br_1 = 100; //1000;
    // let mc_2 = 18; //157;

    let brs = vec![]; // Vec<usize> = (1..14).rev().map(|x| 10 * x).collect();
    let repeats = 50;
    // let brs: Vec<usize> = (1..11).map(|x| 1000 * x).collect();
    for br_1 in brs.iter().cloned() {
        let mc_2 = br_1 * 5 / 12;
        let contract_1 = make_many_methods_contract(2, br_1);
        let funcs_1 = get_func_number(&contract_1);
        let cost_1 = compute_function_call_cost(
            metric,
            vm_kind,
            repeats,
            &contract_1,
            "hello0",
            None,
            vec![],
        );

        let contract_2 = make_many_methods_contract(mc_2, 1);
        let funcs_2 = get_func_number(&contract_2);
        let cost_2 = compute_function_call_cost(
            metric,
            vm_kind,
            repeats,
            &contract_2,
            "hello0",
            None,
            vec![],
        );

        let cost_per_function = Ratio::new(
            max((cost_2 - cost_1) as i128, 0i128),
            repeats as i128 * (funcs_2 - funcs_1) as i128,
        );
        let gas_cost_per_function = ratio_to_gas_signed(metric, cost_per_function);

        println!("SHOULD BE CLOSE: {} {}", contract_1.code().len(), contract_2.code().len());
        println!("funcs: {} {}", funcs_1, funcs_2);
        println!("costs: {} {}", cost_per_function, gas_cost_per_function);
    }

    for (method_count, body_repeat) in vec![
        // (2, 100),
        // (2, 10000),
        // (5, 1),
        // (5, 10),
        // (5, 100),
        // (5, 1000),
        // (20, 10),
        // (20, 100),
        // (50, 1),
        // (50, 10),
        // (50, 100),
        // (100, 10),
        // (100, 100),
        // (200, 10),
        (500, 1),
        (1000, 1),
        (2000, 1),
        (5000, 1),
        // (10000, 1),
    ]
    .iter()
    .cloned()
    {
        // for method_count in vec![5, 20, 30, 50, 100, 200] {
        // for method_count in vec![5, 100, 4500] {
        let contract = make_many_methods_contract(method_count, body_repeat);
        let complexity = get_complexity(&contract);
        let args = vec![];
        println!("LEN = {}", contract.code().len());
        let cost = compute_function_call_cost(
            metric,
            vm_kind,
            REPEATS,
            &contract,
            "hello0",
            None,
            args.clone(),
        );
        let module = cache::wasmer0_cache::compile_module_cached_wasmer0(
            &contract,
            &VMConfig::default(),
            None,
        )
        .unwrap()
        .unwrap();
        let module_info = module.info();
        let funcs = module_info.func_assoc.len();

        println!(
            "{:?} {:?} {} {} {} {} {}",
            vm_kind,
            metric,
            contract.code().len(),
            funcs,
            complexity,
            cost / REPEATS,
            ratio_to_gas_signed(metric, Ratio::new(cost as i128, REPEATS as i128))
        );

        // args_len_xs.push(args.len() as u64);
        // code_len_xs.push(contract.code().len() as u64);
        // funcs_xs.push(funcs as u64);
        // ys.push(cost / REPEATS);

        // data.push(args.len() as f64);
        data.push(contract.code().len() as f64);
        // data.push(funcs as f64);
        data.push(complexity as f64);
        data.push((cost / REPEATS) as f64);

        rows += 1;
    }

    // Regression analysis only makes sense for additive metrics.
    if metric == GasMetric::Time {
        return;
    }

    let COLS = 3;
    let m: DMatrix<f64> = DMatrix::from_row_slice(rows, COLS, &data);

    println!("{:?}", m.shape());
    let xs = m.columns(0, COLS - 1).into_owned();
    let ys = m.column(COLS - 1).into_owned();
    // let (x_train, x_test, y_train, y_test) = train_test_split(&x, &y.transpose(), 0.2, true);

    let (coeff, intercept) = least_squares_method_2(&xs, &ys, COLS);
    println!("coeff: {:?}, intercept: {}", coeff, intercept);
    let mut gas_coeff = vec![];
    for x in coeff.iter().cloned() {
        gas_coeff
            .push(ratio_to_gas_signed(metric, Ratio::new((x * 1_000_000f64) as i128, 1_000_000)));
    }
    let gas_intercept = ratio_to_gas_signed(metric, Ratio::new(intercept as i128, 1));
    println!("gas_coeff: {:?}, gas_intercept = {:?}", gas_coeff, gas_intercept);

    for i in 0..rows {
        let mut gas = gas_intercept;
        for j in 0..COLS - 1 {
            gas += gas_coeff[j] * (xs[(i, j)] as i64);
        }
        let real_gas = ratio_to_gas_signed(metric, Ratio::new(ys[i] as i128, 1));
        println!("est = {}, real = {} | xs = {}", gas, real_gas, xs.row(i));
    }

    // let (cost_base, cost_byte, _) = least_squares_method(&funcs_xs, &ys);

    // println!(
    //     "{:?} {:?} function call base {} gas, per byte {} gas",
    //     vm_kind,
    //     metric,
    //     ratio_to_gas_signed(metric, cost_base),
    //     ratio_to_gas_signed(metric, cost_byte),
    // );
}

#[allow(dead_code)]
fn test_prepare_contract(metric: GasMetric) {
    for (method_count, _) in
        // vec![(2, 1), (5, 1), (10, 1), (100, 1), (1000, 1), (10000, 1)].iter().cloned()
        vec![(10010, 1), (20010, 1), (50010, 1), (100010, 1)].iter().cloned()
    // vec![(0, 0)].iter().cloned()
    {
        let code = many_functions_contract(method_count);
        // let contract = ContractCode::new(code, None);
        let start = start_count(metric);
        let store = RuntimeConfigStore::new(None);
        let config = store.get_config(ProtocolVersion::MAX);
        let vm_config = &config.wasm_config;
        for i in 0..REPEATS {
            print!("{} ", i);
            let result = prepare_contract(&code, vm_config);
            if method_count < 10000 {
                assert!(result.is_ok());
            } else {
                assert!(result.is_err());
            }
        }
        let total_raw = end_count(metric, &start) as i128;

        println!(
            "total cost = {}, average gas cost = {}, len = {}",
            total_raw,
            ratio_to_gas_signed(metric, Ratio::new(total_raw as i128, REPEATS as i128)),
            code.len()
        );
    }
}

#[allow(dead_code)]
fn test_function_call(metric: GasMetric, vm_kind: VMKind) {
    let nftspace_code = vec![];
    // let reader = BufReader::new(
    //     File::open("/host/nearcore/codes.json").expect("Could not open genesis config file."),
    // );
    // let entries: Vec<(Vec<u8>, String)> = serde_json::from_reader(reader).unwrap();
    // let codes: HashMap<String, Vec<u8>> = entries.into_iter().map(|(k, v)| (v, k)).collect();
    // let nftspace_code = codes.get("nftspace.near").unwrap();
    //
    // let m = &mut Module::from_buffer(nftspace_code).unwrap();
    // for i in 0..4000 {
    //     if i % 10 == 0 {
    //         println!("{}", i);
    //     }
    //     let mut hello_func = FunctionBuilder::new(&mut m.types, &[], &[]);
    //     hello_func.func_body().i32_const(1).drop();
    //     let hello_func = hello_func.finish(vec![], &mut m.funcs);
    //     m.exports.add(&format!("hello{}", i), hello_func);
    // }
    // let nftspace_code = m.emit_wasm();

    let mut xs = vec![];
    let mut ys = vec![];

    for (method_count, body_repeat) in
        // vec![(2, 1), (5, 1), (10, 1), (100, 1), (1000, 1), (10000, 1)].iter().cloned()
        // vec![(20000, 1), (20000, 4), (40000, 1)].iter().cloned()
        vec![(5000, 1)].iter().cloned()
    // vec![(0, 0)].iter().cloned()
    {
        let contract = if method_count != 0 {
            make_many_methods_contract(method_count, body_repeat)
        } else {
            ContractCode::new(nftspace_code.clone(), None)
        };

        let args = vec![];
        println!("LEN = {}", contract.code().len());
        let cost = compute_function_call_cost(
            metric,
            vm_kind,
            REPEATS,
            &contract,
            "hello0",
            None,
            args.clone(),
        );

        let module = compile_w2(&contract).unwrap();
        let module_info = module.info();
        let funcs = get_functions_number(contract.code(), &VMConfig::default());
        let funcs2 = module_info.functions.len();

        // let name_table: Vec<(_, _)> = module_info
        //     .function_names
        //     .clone()
        //     .iter()
        //     .filter(|(&k, _)| module_info.is_imported_function(k))
        //     .collect();
        // let imports = module_info.imports.clone();
        // println!("{:?}", imports);
        let exports = module_info.exports.clone();
        println!("{:?}", exports);

        // let table = &module_info.name_table;
        // let imported_funcs: Vec<_> = module_info
        //     .imported_functions
        //     .values()
        //     .map(|i| (namespace_table.get(i.namespace_index), table.get(i.name_index)))
        //     .collect();
        // println!("{:?}", imported_funcs);

        println!(
            "{:?} {:?} {} {} {} {} {}",
            vm_kind,
            metric,
            contract.code().len(),
            funcs,
            funcs2,
            cost / REPEATS,
            ratio_to_gas_signed(metric, Ratio::new(cost as i128, REPEATS as i128))
        );

        xs.push(contract.code().len() as u64);
        ys.push(cost / REPEATS);
    }

    // Regression analysis only makes sense for additive metrics.
    if metric == GasMetric::Time {
        return;
    }

    let (cost_base, cost_byte, _) = least_squares_method(&xs, &ys);

    println!(
        "{:?} {:?} function call base {} gas, per byte {} gas",
        vm_kind,
        metric,
        ratio_to_gas_signed(metric, cost_base),
        ratio_to_gas_signed(metric, cost_byte),
    );
}

fn measure_function_call_1s(vm_kind: VMKind) {
    init_test_logger();

    let (contract, method_name, init_args) = get_rs_contract_data();
    let contract = ContractCode::new(contract.iter().cloned().collect(), None);
    let contract_len = contract.code().len();
    println!("contract length = {}", contract_len);
    println!("method name = {}", method_name);

    let workdir = tempfile::Builder::new().prefix("runtime_testbed").tempdir().unwrap();
    let store = create_store(&get_store_path(workdir.path()));
    let cache_store = Arc::new(StoreCompiledContractCache { store });
    let cache: Option<&dyn CompiledContractCache> = Some(cache_store.as_ref());
    let vm_config = VMConfig::default();
    let mut fake_external = MockedExternal::new();
    let fake_context = create_context(vec![]);
    let fees = RuntimeFeesConfig::test();
    let promise_results = vec![];
    let gas_metric = GasMetric::Time;
    // precompile_contract(&contract, &vm_config, cache);

    let start = start_count(gas_metric);
    let mut i = 0;
    loop {
        let result = run_vm(
            &contract,
            method_name,
            &mut fake_external,
            fake_context.clone(),
            &vm_config,
            &fees,
            &promise_results,
            vm_kind,
            ProtocolVersion::MAX,
            cache,
        );
        i += 1;
        assert!(result.1.is_none());
        if i % 20 == 0 {
            let nanos = end_count(gas_metric, &start) as i128;
            if nanos > 1_000_000_000 {
                break;
            }
        };
    }

    println!("vmkind = {:?}, iters = {}", vm_kind, i);
}

#[test]
fn test_measure_function_call_1s() {
    // Run with
    // cargo test --release --lib function_call::test_function_call_time
    //    --features required  -- --exact --nocapture
    measure_function_call_1s(VMKind::Wasmer0);
    measure_function_call_1s(VMKind::Wasmer2);
    measure_function_call_1s(VMKind::Wasmtime);
}

#[test]
fn test_function_call_time() {
    // init_test_logger();
    tracing_span_tree::span_tree().enable();
    // Run with
    // cargo test --release --lib function_call::test_function_call_time
    //    --features required  -- --exact --nocapture
    // test_function_call(GasMetric::Time, VMKind::Wasmer0);
    test_function_call(GasMetric::Time, VMKind::Wasmer2);
    // test_function_call(GasMetric::Time, VMKind::Wasmtime);
}

#[test]
fn test_function_call_icount() {
    // Use smth like
    // CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER=./runner.sh \
    // cargo test --release --features no_cpu_compatibility_checks,required  \
    // --lib function_call::test_function_call_icount -- --exact --nocapture
    // Where runner.sh is
    // /host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/qemu-x86_64 \
    // -cpu Westmere-v1 -plugin file=/host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/libcounter.so $@
    // test_function_call(GasMetric::ICount, VMKind::Wasmer0);
    test_function_call(GasMetric::ICount, VMKind::Wasmer2);
    // test_function_call(GasMetric::ICount, VMKind::Wasmtime);
}

#[test]
fn test_prepare_contract_icount() {
    // Use smth like
    // CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER=./runner.sh \
    // cargo test --release --features no_cpu_compatibility_checks,required  \
    // --lib function_call::test_prepare_contract_icount -- --exact --nocapture
    // Where runner.sh is
    // /host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/qemu-x86_64 \
    // -cpu Westmere-v1 -plugin file=/host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/libcounter.so $@
    test_prepare_contract(GasMetric::ICount);
}

#[test]
fn compare_function_call_icount() {
    // Base comparison
    // test_function_call(GasMetric::ICount, VMKind::Wasmer0);

    let runtime_fees_config = RuntimeFeesConfig::test();
    let ext_costs_config = ExtCostsConfig::default();

    let old_function_call_fee =
        runtime_fees_config.action_creation_config.function_call_cost.execution;
    println!("old_function_call_fee = {}", old_function_call_fee);

    // let contract_bytes = &include_bytes!("/host/nearcore/aurora_nodata.wat")[..];
    // let wasm_code_cow = wat::parse_bytes(contract_bytes).unwrap().clone();
    // let wasm_code = wasm_code_cow.as_ref().clone();

    let contracts_data = vec![
        get_aurora_small_contract_data(),
        // (wasm_code, "state_migration", None), //get_aurora_small_contract_nodata_data(),
        // get_aurora_with_deploy_data(),
        get_aurora_330_data(),
        get_aurora_contract_data(),
        // get_multisig_contract_data(),
        // get_voting_contract_data(),
        // get_rs_contract_data(),
    ];
    for (i, (contract, method_name, init_args)) in contracts_data.iter().cloned().enumerate() {
        let wat_contract = wabt::wasm2wat(contract).unwrap();
        let path = format!("/host/nearcore/aurora_{}.wat", i);
        std::fs::write(path, wat_contract).expect("Unable to write file");
        println!("{}", method_name);

        // Actual cost
        let contract = ContractCode::new(contract.iter().cloned().collect(), None);
        let contract_len = contract.code().len();
        let funcs = get_func_number(&contract);
        let complexity = get_complexity(&contract);

        println!("contract length = {}", contract_len);

        let cost = compute_function_call_cost(
            GasMetric::ICount,
            VMKind::Wasmer0,
            REPEATS,
            &contract,
            method_name,
            init_args,
            vec![],
        );
        let actual_gas =
            ratio_to_gas_signed(GasMetric::ICount, Ratio::new(cost as i128, REPEATS as i128));
        // println!("actual = {}", actual_gas);

        // Old estimation
        let fee = old_function_call_fee
            + ext_costs_config.contract_compile_base
            + ext_costs_config.contract_compile_bytes * contract_len as u64;
        // runtime_fees_config.action_creation_config.function_call_cost_per_byte is negligible here
        // println!("old estimation = {}", fee);

        // New estimation
        // Prev computed:
        // let new_fee = 37_732_719_837 + 76_128_437 * contract.code().len();
        // Newly:
        // Wasmer0 ICount function call base 48080046101 gas, per byte 207939579 gas
        // let new_fee = 48_080_046_101 + 207_939_579 * contract_len;
        // let new_fee = 8_300_000_000 + 428_000 * contract_len + 2_200_000_000 * funcs;
        let new_fee = 2 * (293_728 * contract_len + 2_040_243_966 * complexity);

        // println!("new estimation = {}", new_fee);

        println!(
            "{}, funcs = {}, complexity = {}, len = {}, actual gas = {}, old fee = {}, new fee = {}",
            method_name, funcs, complexity, contract_len, actual_gas, fee, new_fee
        );
    }
}

fn make_many_methods_contract(method_count: usize, body_repeat: usize) -> ContractCode {
    assert!(method_count > 1);
    let mut methods = String::new();
    // let imports = [
    //     ("env", "log_utf8"),
    //     ("env", "promise_create"),
    //     ("env", "storage_read"),
    //     ("env", "register_len"),
    //     ("env", "block_timestamp"),
    //     ("env", "block_index"),
    //     ("env", "read_register"),
    //     ("env", "keccak256"),
    //     ("env", "ecrecover"),
    //     ("env", "sha256"),
    //     ("env", "current_account_id"),
    //     ("env", "predecessor_account_id"),
    //     ("env", "storage_write"),
    //     ("env", "storage_has_key"),
    //     ("env", "value_return"),
    //     ("env", "promise_then"),
    //     ("env", "promise_return"),
    //     ("env", "storage_remove"),
    //     ("env", "signer_account_id"),
    //     ("env", "ripemd160"),
    //     ("env", "input"),
    //     ("env", "panic_utf8"),
    //     ("env", "promise_batch_create"),
    //     ("env", "promise_batch_action_deploy_contract"),
    //     ("env", "promise_batch_action_function_call"),
    //     ("env", "promise_results_count"),
    //     ("env", "promise_result"),
    //     ("env", "attached_deposit"),
    //     ("env", "promise_batch_action_transfer"),
    //     ("env", "gas"),
    // ];
    // for i in imports.iter().cloned() {
    //     write!(
    //         &mut methods,
    //         "
    //                 (import \"{}\" \"{}\" (memory 1))
    //         ",
    //         i.0, i.1
    //     )
    //     .unwrap();
    // }
    //   write!(
    //       &mut methods,
    //       "
    // (type (;0;) (func (param i32 i32 i32 i64 i64 i32 i32)))
    // (type (;1;) (func (param i32 i32 i32 i32)))
    // (type (;2;) (func (param i32 i32) (result i32)))
    // (type (;3;) (func (param i32 i32)))
    // (type (;4;) (func (param i32)))
    // (type (;5;) (func (param i32 i32 i32) (result i32)))
    // (type (;6;) (func (param i64 i64)))
    // (type (;7;) (func (param i64 i64 i64 i64 i64 i64 i64 i64) (result i64)))
    // (type (;8;) (func (param i64 i64 i64) (result i64)))
    // (type (;9;) (func (param i64) (result i64)))
    // (type (;10;) (func (result i64)))
    // (type (;11;) (func (param i64 i64 i64)))
    // (type (;12;) (func (param i64 i64 i64 i64 i64 i64 i64) (result i64)))
    // (type (;13;) (func (param i64)))
    // (type (;14;) (func (param i64 i64 i64 i64 i64) (result i64)))
    // (type (;15;) (func (param i64 i64) (result i64)))
    // (type (;16;) (func (param i64 i64 i64 i64 i64 i64 i64 i64 i64) (result i64)))
    // (type (;17;) (func (param i64 i64 i64 i64 i64 i64 i64)))
    // (type (;18;) (func (param i32 i32 i32)))
    // (type (;19;) (func (param i32) (result i32)))
    // (type (;20;) (func (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32 i32)))
    // (type (;21;) (func (param i32 i32 i32 i32 i32 i32 i64 i64 i32)))
    // (type (;22;) (func (param i32 i32 i64)))
    // (type (;23;) (func (param i32 i32 i32 i32 i32 i32 i64 i32)))
    // (type (;24;) (func (param i32) (result i64)))
    // (type (;25;) (func (param i32 i32 i32 i32 i32 i32 i32)))
    // (type (;26;) (func (param i32 i32 i32 i32 i32)))
    // (type (;27;) (func (param i32 i32 i64 i64)))
    // (type (;28;) (func (param i32 i32 i32 i32 i32 i32)))
    // (type (;29;) (func (param i32 i32 i32 i32 i32 i64 i64 i32)))
    // (type (;30;) (func (param i32 i32 i64 i32 i32)))
    // (type (;31;) (func (param i32 i32 i32 i64 i32) (result i32)))
    // (type (;32;) (func (param i32 i32 i32 i32 i32 i64 i32)))
    // (type (;33;) (func (param i32 i32 i32 i32 i32) (result i32)))
    // (type (;34;) (func))
    // (type (;35;) (func (param i32 i64)))
    // (type (;36;) (func (param i32 i32 i32 i32) (result i32)))
    // (type (;37;) (func (param i32 i32 i32 i64)))
    // (type (;38;) (func (param i32 i64 i32)))
    // (type (;39;) (func (param i32 i64 i64)))
    // (type (;40;) (func (param i32 i32 i32 i32 i32 i32 i64 i64 i64)))
    // (type (;41;) (func (param i32 f64 i32 i32) (result i32)))
    // (type (;42;) (func (param i64 i32 i32)))
    // (type (;43;) (func (param i64 i64 i32 i32) (result i32)))
    // (type (;44;) (func (param i32 i32 i32 i32 i32 i32) (result i32)))
    // (type (;45;) (func (param i32 i32 i32) (result i64)))
    // (type (;46;) (func (param i64 i32 i32) (result i32)))
    // (type (;47;) (func (param f64 i32) (result f64)))
    // (type (;48;) (func (param i32 i64 i64 i64 i64 i32)))
    // (type (;49;) (func (param i32 i64 i64 i64 i64)))
    // (type (;50;) (func (param f64 f64) (result f64)))
    // (import \"env\" \"log_utf8\" (func (;0;) (type 6)))
    // (import \"env\" \"promise_create\" (func (;1;) (type 7)))
    // (import \"env\" \"storage_read\" (func (;2;) (type 8)))
    // (import \"env\" \"register_len\" (func (;3;) (type 9)))
    // (import \"env\" \"block_timestamp\" (func (;4;) (type 10)))
    // (import \"env\" \"block_index\" (func (;5;) (type 10)))
    // (import \"env\" \"read_register\" (func (;6;) (type 6)))
    // (import \"env\" \"keccak256\" (func (;7;) (type 11)))
    // (import \"env\" \"ecrecover\" (func (;8;) (type 12)))
    // (import \"env\" \"sha256\" (func (;9;) (type 11)))
    // (import \"env\" \"current_account_id\" (func (;10;) (type 13)))
    // (import \"env\" \"predecessor_account_id\" (func (;11;) (type 13)))
    // (import \"env\" \"storage_write\" (func (;12;) (type 14)))
    // (import \"env\" \"storage_has_key\" (func (;13;) (type 15)))
    // (import \"env\" \"value_return\" (func (;14;) (type 6)))
    // (import \"env\" \"promise_then\" (func (;15;) (type 16)))
    // (import \"env\" \"promise_return\" (func (;16;) (type 13)))
    // (import \"env\" \"storage_remove\" (func (;17;) (type 8)))
    // (import \"env\" \"signer_account_id\" (func (;18;) (type 13)))
    //       "
    //   );
    /*
    (import \"env\" \"ripemd160\" (func (;19;) (type 11)))
    (import \"env\" \"input\" (func (;20;) (type 13)))
    (import \"env\" \"panic_utf8\" (func (;21;) (type 6)))
    (import \"env\" \"promise_batch_create\" (func (;22;) (type 15)))
    (import \"env\" \"promise_batch_action_deploy_contract\" (func (;23;) (type 11)))
    (import \"env\" \"promise_batch_action_function_call\" (func (;24;) (type 17)))
    (import \"env\" \"promise_results_count\" (func (;25;) (type 10)))
    (import \"env\" \"promise_result\" (func (;26;) (type 15)))
    (import \"env\" \"attached_deposit\" (func (;27;) (type 13)))
    (import \"env\" \"promise_batch_action_transfer\" (func (;28;) (type 6)))
      */
    for i in 0..method_count {
        let mut body = String::new();
        write!(&mut body, "i32.const {i} drop ", i = i).unwrap();
        if i > 0 {
            body = body.repeat(body_repeat);
        }
        if i == 0 {
            write!(&mut methods, "(export \"hello{i}\" (func {index}))", i = i, index = i + 0)
                .unwrap();
        }
        write!(
            &mut methods,
            "            
              (func (;{index};)
                {body}
                return
              )
            ",
            index = i + 0,
            body = body,
        )
        .unwrap();
    }
    let code = format!(
        "
        (module
            {}
            )",
        methods
    );
    ContractCode::new(wat::parse_str(code).unwrap(), None)
}

pub fn compute_function_call_cost(
    gas_metric: GasMetric,
    vm_kind: VMKind,
    repeats: u64,
    contract: &ContractCode,
    method_name: &str,
    init_args: Option<Vec<u8>>,
    args: Vec<u8>,
) -> u64 {
    let workdir = tempfile::Builder::new().prefix("runtime_testbed").tempdir().unwrap();
    let store = create_store(&get_store_path(workdir.path()));
    let cache_store = Arc::new(StoreCompiledContractCache { store });
    let cache: Option<&dyn CompiledContractCache> = Some(cache_store.as_ref());
    let vm_config = VMConfig::default();
    let mut fake_external = MockedExternal::new();
    let fake_context = create_context(args);
    let fees = RuntimeFeesConfig::test();
    let promise_results = vec![];
    // precompile_contract(&contract, &vm_config, cache);

    match init_args {
        Some(args) => {
            let mut init_context = create_context(args);
            init_context.attached_deposit = 0;
            let result = run_vm(
                &contract,
                "new",
                &mut fake_external,
                init_context,
                &vm_config,
                &fees,
                &promise_results,
                vm_kind,
                ProtocolVersion::MAX,
                cache,
            );
            if result.1.is_some() {
                println!("{:?}", result);
                return 0u64;
            }
            assert!(result.1.is_none());
        }
        None => {}
    };

    // Warmup.
    if repeats != 1 {
        let result = run_vm(
            &contract,
            method_name,
            &mut fake_external,
            fake_context.clone(),
            &vm_config,
            &fees,
            &promise_results,
            vm_kind,
            ProtocolVersion::MAX,
            cache,
        );
        if result.1.is_some() {
            println!("{:?}", result);
            return 0u64;
        }
        assert!(result.1.is_none());
    }
    // Run with gas metering.
    let start = start_count(gas_metric);
    for i in 0..repeats {
        print!("{} ", i);
        let result = run_vm(
            &contract,
            method_name,
            &mut fake_external,
            fake_context.clone(),
            &vm_config,
            &fees,
            &promise_results,
            vm_kind,
            ProtocolVersion::MAX,
            cache,
        );
        assert!(result.1.is_none());
    }
    let total_raw = end_count(gas_metric, &start) as i128;

    println!(
        "total cost = {}, average gas cost = {}",
        total_raw,
        ratio_to_gas_signed(gas_metric, Ratio::new(total_raw as i128, repeats as i128))
    );

    total_raw as u64
}
