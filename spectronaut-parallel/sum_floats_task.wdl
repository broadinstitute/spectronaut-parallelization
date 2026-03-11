task sum_floats {
    input {
        Array[Float] sizes
    }

    command <<<
        python3 <<CODE
        sizes = [~{sep="," sizes}]
        print(sum(sizes))
        CODE
    >>>

    output {
        Float total_size = read_float(stdout())
    }

    runtime {
        docker: "python:3.9-slim"
        cpu: 1
        memory: "2GB"
        preemptible: 2
    }
}
