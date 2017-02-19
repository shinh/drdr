require 'thread'

class DRError < RuntimeError
end

class DRTask
  attr_accessor :inputs, :outputs
  attr_reader :name, :tid, :ckpt, :run, :done, :result

  def initialize(name=nil, tid=0, ckpt: nil, &proc)
    @inputs = []
    @outputs = []
    @proc = proc
    @tid = tid
    @name = name
    @ckpt = ckpt
    @run = false
    @done = false
    @debug = false
  end

  def |(r)
    tasks.each do |it|
      r.tasks.each do |ot|
        ot.inputs << it
        it.outputs << ot
      end
    end
    r
  end

  def +(r)
    DRTaskGroup.new([*tasks, *r.tasks])
  end

  def tasks
    [self]
  end

  def to_s
    n = @tid != @name ? "(#{@name})" : ""
    "Task#{@tid}#{n}"
  end

  def inspect
    ins = inputs.map{|i|i.name} * " "
    "#{to_s}: #{ins}"
  end

  def can_run
    !@run && @inputs.all?{|i|i.done}
  end

  def maybe_skip
    if @ckpt
      if File.exist?(@ckpt)
        @done = true
        File.open(@ckpt) do |f|
          @result = Marshal.load(f)
        end
        return true
      end
    end
    false
  end

  def start
    @run = true
  end

  def finish(r)
    if @ckpt
      File.open(@ckpt, 'w'){|of|
        Marshal.dump(r, of)
      }
    end

    @done = true
    @result = r
  end

  def run
    ins = @inputs.map{|i|i.result}
    @proc.call(*ins)
  end

end

class DRTaskGroup < DRTask
  attr_accessor :tasks

  def initialize(tasks)
    @tasks = tasks
  end

  def inspect
    ts = tasks.map{|i|i.name} * " "
    "TaskGroup(#{ts})"
  end
end

class DRGraph

  def initialize(log: STDERR, &proc)
    @proc = proc
    @tasks = {}
    @tid = 0
    @thid = 0
    @log = log

    @threads = {}
    @mu = Mutex.new
    @cond = ConditionVariable.new

    @results = *instance_eval(&@proc)
  end

  def run
    if @tasks.empty?
      @log << "DR: No task in the graph\n"
      return
    end

    @log << "DR: execute graph with #{@tasks.size} tasks\n"
    STDERR.puts "DR: About to execute a graph:\n#{inspect}\n\n" if @debug

    analyze

    run_loop
    @threads.each do |_, th|
      th.join
    end

    results = @results.map do |r|
      if r.is_a? DRTask
        r.result
      else
        r
      end
    end
    results.size == 1 ? results[0] : results
  end

  def traverse(task, seen, ntasks)
    raise DRError.new("Cyclic dependency detected") if seen[task]
    seen[task] = true

    if task.maybe_skip
      @log << "DR: there is a ckpt #{task.ckpt} for #{task}\n"
      return
    end
    ntasks[task.tid] = task

    task.inputs.each do |it|
      traverse(it, seen, ntasks)
    end
    seen[task] = false
  end

  def analyze
    goals = []
    @tasks.each do |_, task|
      if task.outputs.empty?
        goals << task
      end
    end

    ntasks = {}
    raise DRError.new("Cyclic dependency detected") if goals.empty?
    goals.each do |goal|
      traverse(goal, {}, ntasks)
    end

    if @tasks.size != ntasks.size
      diff = @tasks.size - ntasks.size
      @log << "DR: #{diff} tasks were skipped thanks to ckpts\n"
      @tasks = ntasks
    end
  end

  def run_loop
    loop do
      @mu.synchronize do
        launch_tasks
        return if @threads.empty?
        @cond.wait(@mu)
        raise @exception if @exception
      end
    end
  end

  def launch_tasks
    @tasks.each do |_, task|
      if task.can_run
        @log << "DR: start #{task}\n"
        STDERR.puts "DR: start #{task.inspect}" if @debug
        task.start
        thid = @thid += 1
        th = Thread.start do
          run_task(task, thid)
        end
        @threads[thid] = th
      end
    end
  end

  def run_task(task, thid)
    begin
      result = task.run
    rescue => e
      @mu.synchronize do
        @exception = e
        @cond.signal
        return
      end
    end

    @mu.synchronize do
      STDERR.puts "DR: finish (#{result}) #{task.inspect}" if @debug
      task.finish(result)
      @threads.delete(thid)
      @cond.signal
    end
  end

  def task(name=nil, **kwargs, &proc)
    @mu.synchronize do
      @tid += 1
      name ||= @tid
      task = @tasks[@tid] = DRTask.new(name, @tid, **kwargs, &proc)
      @cond.signal
      task
    end
  end

  def shell_escape(s)
    s.gsub('\\', '\\\\').gsub("'", '\\\'')
  end

  def cmd(args, name=nil, **kwargs)
    name ||= "'#{[*args].map{|a|shell_escape(a)} * ' '}'"
    task(name, **kwargs) do |*ins|
      if ins.size > 1
        raise DRError.new("`cmd` takes only a single input but comes #{ins}")
      elsif ins.size == 1
        instr = ins[0].to_s
      else
        instr = ''
      end
      pipe = IO.popen(args, 'r+:binary')
      pipe.print instr
      pipe.close_write
      result = pipe.read
      pipe.close
      if !$?.success?
        msg = "cmd #{name} failed (status=#{$?.exitstatus})"
        raise DRError.new(msg)
      end
      result
    end
  end

  def show
    puts inspect
  end

  def inspect
    @tasks.map do |_, task|
      task.inspect
    end * "\n"
  end

  def debug
    @debug = true
  end

end


def drdr(log: STDERR, &proc)
  DRGraph.new(log: log, &proc).run
end


if $0 == __FILE__
  require 'test/unit'
  require 'fileutils'
  require 'tmpdir'

  class DrdrTest < Test::Unit::TestCase

    def setup
      @testdir = "#{Dir.tmpdir}/drdr_test"
      FileUtils.rm_r(@testdir)
      FileUtils.mkdir_p(@testdir)
      Dir.chdir(@testdir)
    end

    def test_drdr
      assert_equal (42/2)+(42*2), drdr {
        task{ 42 } | task{|x|x / 2} + task{|x|x * 2} | task{|x, y|x + y}
      }
    end

    def test_access_local
      x = nil
      y = nil
      drdr {
        task{ x = 42 }
        task{ y = 99 }
      }
      assert_equal 42, x
      assert_equal 99, y
    end

    def test_access_local2
      x = nil
      y = nil
      drdr {
        task{ x = 42 } + task{ y = 99 }
      }
      assert_equal 42, x
      assert_equal 99, y
    end

    def test_add_task
      x = 0
      drdr {
        task{
          1.upto(10){|i|
            task{ x += i }
          }
        }
      }
      assert_equal 55, x
    end

    class TestError < RuntimeError
    end

    class ShouldntHappen < RuntimeError
    end

    def test_raise
      assert_raise TestError do
        drdr {
          task{ raise TestError.new } | task{ raise ShouldntHappen.new }
        }
      end
    end

    def test_log
      log = ''
      drdr(log: log) {
        task('hoge'){} | task('fuga'){}
      }
      assert_match /hoge.*fuga/m, log
    end

    def test_cmd
      assert_equal "fxo\n", drdr {
        cmd(%W(echo foo)) | cmd(%W(sed s/o/x/))
      }
    end

    def test_cmd_fail
      assert_raise DRError do
        drdr {
          cmd("false") | task{ raise ShouldntHappen.new }
        }
      end
    end

    def test_cyclic
      assert_raise DRError do
        drdr {
          a = task{}
          b = task{}
          a | b
          b | a
        }
      end
    end

    def test_cyclic2
      assert_raise DRError do
        drdr {
          a = task{}
          b = task{}
          c = task{}
          a | b | c
          b | a
        }
      end
    end

    def test_ckpt
      assert_equal "foo\n", drdr {
        cmd("echo foo", ckpt: "foo") | task{|i|i}
      }
      assert_true File.exist?("foo")

      assert_equal "foo\nbar", drdr {
        task(ckpt: "foo"){ raise ShouldntHappen.new } | task{|x|x + "bar"}
      }
    end

    def test_variable
      assert_equal ["foo", "barbaz", 42], drdr {
        foo = task{"foo"}
        barbaz = task{"bar"} | task{|x|x+"baz"}
        [foo, barbaz, 42]
      }
    end

    def test_nest_drdr
      assert_equal "foo", drdr {
        task { drdr { task { "foo" } } }
      }
      assert_equal "foobar", drdr {
        task { drdr { task { "foo" } } } | task{|x|x + "bar"}
      }
    end

    # TODO: This is probably a nice-to-have, but I'm not sure how we
    # implement this.
    # def test_nest_task
    #   assert_equal "foo", drdr {
    #     task { task { "foo" } }
    #   }
    #   assert_equal "foobar", drdr {
    #     task { task { "foo" } } | task{|x|x + "bar"}
    #   }
    # end

    def test_empty_drdr
      drdr {}
    end

  end
end
