use crate::cases::ratio_to_gas_signed;
use crate::testbed_runners::{end_count, start_count, Consumed, GasMetric};
use crate::vm_estimator::{create_context, least_squares_method, least_squares_method_2};
use nalgebra::{DMatrix, DVector, Matrix, Matrix2x3, MatrixXx4, RowVector, Vector, Vector2};
use near_logger_utils::init_test_logger;
use near_primitives::config::VMConfig;
use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::{CompiledContractCache, ProtocolVersion};
use near_store::{create_store, StoreCompiledContractCache};
use near_test_contracts::{
    aurora_contract, get_aurora_contract_data, get_multisig_contract_data, get_rs_contract_data,
    get_voting_contract_data,
};
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::ExtCostsConfig;
use near_vm_runner::cache;
use near_vm_runner::{precompile_contract, run_vm, VMKind};
use nearcore::get_store_path;
use num_rational::Ratio;
use std::cmp::max;
use std::fmt::Write;
use std::sync::Arc;

const REPEATS: u64 = 50;

fn get_func_number(contract: &ContractCode) -> usize {
    let module =
        cache::wasmer0_cache::compile_module_cached_wasmer0(&contract, &VMConfig::default(), None)
            .unwrap();
    let module_info = module.info();
    module_info.func_assoc.len()
}

#[allow(dead_code)]
fn test_function_call(metric: GasMetric, vm_kind: VMKind) {
    // let (mut args_len_xs, mut code_len_xs, mut funcs_xs) = (vec![], vec![], vec![]);
    // let mut ys = vec![];
    let mut rows = 0;
    let mut data = Vec::new();

    // let br_1 = 100; //1000;
    // let mc_2 = 18; //157;

    let brs: Vec<usize> = (1..11).rev().map(|x| 10 * x).collect();
    let repeats = 100;
    // let brs: Vec<usize> = (1..11).map(|x| 100 + x * 20).collect();
    for br_1 in brs.iter().cloned() {
        let mc_2 = br_1 / 6 + 2;
        let contract_1 = make_many_methods_contract(1, br_1);
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
        println!("costs: {} {}", cost_per_function, gas_cost_per_function);
    }

    for (method_count, body_repeat) in vec![
        (1, 100),
        (1, 10000),
        (5, 1),
        (5, 10),
        (5, 100),
        (5, 1000),
        (20, 10),
        (20, 100),
        (50, 1),
        (50, 100),
        (200, 10),
        (1000, 1),
        (2000, 1),
        (5000, 1),
    ]
    .iter()
    .cloned()
    {
        // for method_count in vec![5, 20, 30, 50, 100, 200] {
        // for method_count in vec![5, 100, 4500] {
        let contract = make_many_methods_contract(method_count, body_repeat);
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
        .unwrap();
        let module_info = module.info();
        let funcs = module_info.func_assoc.len();

        println!(
            "{:?} {:?} {} {} {} {}",
            vm_kind,
            metric,
            contract.code().len(),
            funcs,
            cost / REPEATS,
            ratio_to_gas_signed(metric, Ratio::new(cost as i128, REPEATS as i128))
        );

        // args_len_xs.push(args.len() as u64);
        // code_len_xs.push(contract.code().len() as u64);
        // funcs_xs.push(funcs as u64);
        // ys.push(cost / REPEATS);

        // data.push(args.len() as f64);
        data.push(contract.code().len() as f64);
        data.push(funcs as f64);
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
    let fees = RuntimeFeesConfig::default();
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
    measure_function_call_1s(VMKind::Wasmer1);
    measure_function_call_1s(VMKind::Wasmtime);
}

#[test]
fn test_function_call_time() {
    // Run with
    // cargo test --release --lib function_call::test_function_call_time
    //    --features required  -- --exact --nocapture
    test_function_call(GasMetric::Time, VMKind::Wasmer0);
    // test_function_call(GasMetric::Time, VMKind::Wasmer1);
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
    test_function_call(GasMetric::ICount, VMKind::Wasmer0);
    // test_function_call(GasMetric::ICount, VMKind::Wasmer1);
    // test_function_call(GasMetric::ICount, VMKind::Wasmtime);
}

#[test]
fn compare_function_call_icount() {
    // Base comparison
    // test_function_call(GasMetric::ICount, VMKind::Wasmer0);

    let runtime_fees_config = RuntimeFeesConfig::default();
    let ext_costs_config = ExtCostsConfig::default();

    let old_function_call_fee =
        runtime_fees_config.action_creation_config.function_call_cost.execution;
    println!("old_function_call_fee = {}", old_function_call_fee);

    let contracts_data = vec![
        get_aurora_contract_data(),
        get_multisig_contract_data(),
        get_voting_contract_data(),
        get_rs_contract_data(),
    ];
    for (contract, method_name, init_args) in contracts_data.iter().cloned() {
        println!("{}", method_name);

        // Actual cost
        let contract = ContractCode::new(contract.iter().cloned().collect(), None);
        let contract_len = contract.code().len();
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
        let new_fee = 48_080_046_101 + 207_939_579 * contract_len;

        // println!("new estimation = {}", new_fee);

        println!("{},{},{},{},{}", method_name, contract_len, actual_gas, fee, new_fee);
    }
}

fn make_many_methods_contract(method_count: usize, body_repeat: usize) -> ContractCode {
    let mut methods = String::new();
    for i in 0..method_count {
        let mut body = String::new();
        write!(&mut body, "i32.const {i} drop ", i = i).unwrap();
        if body_repeat > 0 && (i > 0 || method_count == 1) {
            body = body.repeat(body_repeat);
        }
        write!(
            &mut methods,
            "
            (export \"hello{}\" (func {i}))
              (func (;{i};)
                {body}
                return
              )
            ",
            i = i,
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
    let fees = RuntimeFeesConfig::default();
    let promise_results = vec![];
    precompile_contract(&contract, &vm_config, cache);

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
    for _ in 0..repeats {
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
