defmodule Fairway.BootTest do
  # Acceptance criterion E: an invalid YAML file is rejected at boot with an
  # error naming the bad key. "Boot" is tested both ways Fairway can be
  # started: in a supervision tree, and by its own application.
  use Fairway.Test.Case, async: false

  import ExUnit.CaptureLog

  alias Fairway.Config.Error
  alias Fairway.Test.Workers.Echo

  @moduletag :tmp_dir

  @invalid """
  queues:
    emails:
      mode: per_tenant
      concurency: 8
  """

  @valid """
  queues:
    q:
      mode: per_tenant
      concurrency: 2
  """

  defp write!(tmp_dir, yaml) do
    path = Path.join(tmp_dir, "fairway.yml")
    File.write!(path, yaml)
    path
  end

  describe "in a supervision tree" do
    test "an invalid file is rejected with an error naming the bad key", %{tmp_dir: tmp_dir} do
      path = write!(tmp_dir, @invalid)

      assert {:error, %Error{path: "queues.emails.concurency"} = error} =
               Fairway.start_link(config_file: path)

      assert Exception.message(error) =~
               "invalid Fairway configuration at queues.emails.concurency: unknown key"

      # Nothing was started.
      assert Process.whereis(Fairway.Supervisor) == nil
      assert Fairway.Supervisor.config() == :error
    end

    test "the supervisor above it fails to start, with the same error", %{tmp_dir: tmp_dir} do
      path = write!(tmp_dir, @invalid)

      assert {:error, {%Error{path: "queues.emails.concurency"}, _child}} =
               start_supervised({Fairway, config_file: path})
    end

    test "a file that is missing or is not YAML is rejected too", %{tmp_dir: tmp_dir} do
      assert {:error, %Error{path: nil, reason: "cannot read" <> _}} =
               Fairway.start_link(config_file: Path.join(tmp_dir, "absent.yml"))

      assert {:error, %Error{path: nil, reason: reason}} =
               Fairway.start_link(config_file: write!(tmp_dir, "queues: {q: [unclosed"))

      assert reason =~ "cannot read"
    end

    test "a valid file starts Fairway, which runs jobs", %{tmp_dir: tmp_dir} do
      forward_telemetry()
      start_supervised!({Fairway, config_file: write!(tmp_dir, @valid)})

      {:ok, [_job]} = Fairway.enqueue_all(jobs("acme", 1, Echo))
      await_stops("acme", :completed, 1)
    end

    test "wrong options are a programming error" do
      assert_raise ArgumentError, fn -> Fairway.start_link([]) end
      assert_raise ArgumentError, fn -> Fairway.start_link(config: %{}) end
    end
  end

  describe "as the :fairway application" do
    setup do
      # The application is running, with no config_file, because the test suite
      # depends on it. Stop it, and put things back afterwards. Stopping an
      # application is logged by OTP itself; that is captured, not printed.
      capture_log(fn -> :ok = Application.stop(:fairway) end)

      on_exit(fn ->
        capture_log(fn -> Application.stop(:fairway) end)
        Application.delete_env(:fairway, :config_file)
        {:ok, _apps} = Application.ensure_all_started(:fairway)
      end)
    end

    test "an invalid file stops the application from booting, with an error naming the bad key",
         %{tmp_dir: tmp_dir} do
      Application.put_env(:fairway, :config_file, write!(tmp_dir, @invalid))

      assert {:error,
              {:fairway,
               {{:invalid_config, message}, {Fairway.Application, :start, [:normal, []]}}}} =
               Application.ensure_all_started(:fairway)

      assert message =~ "queues.emails.concurency"
      assert Process.whereis(Fairway.Supervisor) == nil
    end

    test "a valid file boots Fairway, which runs jobs", %{tmp_dir: tmp_dir} do
      forward_telemetry()
      Application.put_env(:fairway, :config_file, write!(tmp_dir, @valid))

      assert {:ok, [:fairway]} = Application.ensure_all_started(:fairway)
      assert is_pid(Process.whereis(Fairway.Supervisor))

      {:ok, [_job]} = Fairway.enqueue_all(jobs("acme", 1, Echo))
      await_stops("acme", :completed, 1)
    end
  end
end
