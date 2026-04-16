# [
#   {
#     $lookup: {
#       from: "payroll",
#       localField: "payroll_id",
#       foreignField: "_id",
#       pipeline: [{ $project: { name: 1 } }],
#       as: "payroll_details"
#     }
#   },
#   {
#     $lookup: {
#       from: "payroll_period_details",
#       localField: "period_id",
#       foreignField: "_id",
#       pipeline: [
#         { $project: { period_name: 1 } }
#       ],
#       as: "period_details"
#     }
#   },
#   {
#     $lookup: {
#       from: "payroll_runs_employees",
#       let: { run_id: "$_id" },
#       pipeline: [
#         {
#           $match: {
#             $expr: {
#               $eq: ["$run_id", "$$run_id"]
#             }
#           }
#         },
#         {
#           $lookup: {
#             from: "employees",
#             localField: "employee_id",
#             foreignField: "_id",
#             pipeline: [
#               { $project: { full_name: 1 } }
#             ],
#             as: "employee_details"
#           }
#         },
#         {
#           $lookup: {
#             from: "payroll_runs_employees_elements",
#             let: { current_employee_id: "$_id" },
#             pipeline: [
#               {
#                 $match: {
#                   $expr: {
#                     $eq: [
#                       "$run_employee_id",
#                       "$$current_employee_id"
#                     ]
#                   }
#                 }
#               },
#
#               // ✅ 1. get employee payroll element
#               {
#                 $lookup: {
#                   from: "employees_payrolls",
#                   localField: "element_id",
#                   foreignField: "_id",
#                   as: "employee_payrolls_details"
#                 }
#               },
#
#               // ✅ 2. extract real element_id
#               {
#                 $addFields: {
#                   payroll_element_id: {
#                     $first:
#                       "$employee_payrolls_details.name"
#                   }
#                 }
#               },
#
#               // ✅ 3. lookup actual payroll element
#               {
#                 $lookup: {
#                   from: "payroll_elements",
#                   localField:
#                     "payroll_element_id",
#                   foreignField: "_id",
#                   as: "element_details"
#                 }
#               },
#
#               // ✅ 4. get type
#               {
#                 $addFields: {
#                   element_type: {
#                     $first:
#                       "$element_details.type"
#                   }
#                 }
#               },
#
#               // ✅ 5. classify values
#               {
#                 $addFields: {
#                   payment: {
#                     $cond: [
#                       {
#                         $eq: [
#                           "$element_type",
#                           "Earning"
#                         ]
#                       },
#                       "$value",
#                       0
#                     ]
#                   },
#                   deduction: {
#                     $cond: [
#                       {
#                         $eq: [
#                           "$element_type",
#                           "Deduction"
#                         ]
#                       },
#                       "$value",
#                       0
#                     ]
#                   }
#                 }
#               }
#
#               // ✅ cleanup
#               // {
#               //   $project: {
#               //     element_details: 0,
#               //     element_type: 0,
#               //     employee_payrolls_details: 0,
#               //     payroll_element_id: 0
#               //   }
#               // }
#             ],
#             as: "run_employee_details"
#           }
#         },
#         {
#           $addFields: {
#             employee_name: {
#               $first:
#                 "$employee_details.full_name"
#             },
#             total_payments: {
#               $sum: "$run_employee_details.payment"
#             },
#             total_deductions: {
#               $sum: "$run_employee_details.deduction"
#             },
#             net_salary: {
#               $subtract: [
#                 {
#                   $sum: "$run_employee_details.payment"
#                 },
#                 {
#                   $sum: "$run_employee_details.deduction"
#                 }
#               ]
#             }
#           }
#         },
#         {
#           $project: {
#             employee_details: 0
#           }
#         }
#       ],
#       as: "employees_details"
#     }
#   },
#   {
#     $addFields: {
#       payroll_name: {
#         $first: "$payroll_details.name"
#       },
#       period_name: {
#         $first: "$period_details.period_name"
#       }
#     }
#   },
#   {
#     $project: {
#       _id: {
#         $toString: "$_id"
#       },
#       run_number: 1,
#       description: 1,
#       payment_number: 1,
#       employees_details: 1,
#       payroll_name: 1,
#       period_name: 1
#     }
#   }
# ]